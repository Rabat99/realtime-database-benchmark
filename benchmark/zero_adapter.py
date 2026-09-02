# -*- coding: utf-8 -*-
"""
Zero-Subscriber-Pool.

Im normalen Benchmark-Modus startet der Pool wenige Zero-Client-Prozesse
und verteilt viele DISTINCT materialisierte Queries auf diese Prozesse.

Fuer den Subscription-Init-Benchmark kann optional ein Barrier-Modus
aktiviert werden. Dabei verbinden sich zuerst alle Client-Prozesse mit
Zero, melden anschliessend "connected" und "armed" und warten auf ein
gemeinsames GO-Signal ueber stdin.

Ohne Barrier-Modus arbeitet der Pool im Steady-State-Modus.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from pathlib import Path
from typing import Any


CONTAINER_PREFIX = "zero-bench-"


class ZeroSubscriberPool:
    def __init__(
        self,
        *,
        repo_root: Path,
        total_queries: int,
        clients: int = 4,
        query_class: str = "window",
        topics: int = 30000,
        limit: int = 10,
        offset: int = 5,
        needle: str = "gamma",
        emit_rows: bool = False,
        log_buffer: int = 500,
    ) -> None:
        if total_queries < 1:
            raise ValueError(
                "total_queries must be >= 1"
            )

        if clients < 1:
            raise ValueError(
                "clients must be >= 1"
            )

        if clients > total_queries:
            clients = total_queries

        self.repo_root = repo_root
        self.total_queries = total_queries
        self.clients = clients
        self.query_class = query_class
        self.topics = topics
        self.limit = limit
        self.offset = offset
        self.needle = needle
        self.emit_rows = emit_rows

        self.processes: list[
            asyncio.subprocess.Process
        ] = []

        self.read_tasks: list[
            asyncio.Task
        ] = []

        self._connected_clients: set[str] = set()
        self._connected_event = asyncio.Event()

        self._armed_clients: set[str] = set()
        self._armed_event = asyncio.Event()

        self._released_clients: set[str] = set()

        self._ready_clients: set[str] = set()
        self._ready_event = asyncio.Event()

        self._pending: dict[
            str,
            asyncio.Future,
        ] = {}

        self._orphans: dict[
            str,
            dict[str, Any],
        ] = {}

        self._closing = False
        self._started = False
        self._barrier_mode = False
        self._released = False

        self._release_epoch_ns: int | None = None

        self.errors: list[
            dict[str, Any]
        ] = []

        self.rows: dict[
            int,
            list[int],
        ] = {}

        self.row_snapshots: dict[
            int,
            list[dict[str, Any]],
        ] = {}

        self.rows_changed = asyncio.Event()

        self.logs: deque[str] = deque(
            maxlen=log_buffer
        )

        self.loop_lag_ms: dict[
            str,
            float,
        ] = {}

        self.progress: dict[
            str,
            int,
        ] = {}

        self.missed_probes = 0

    # ------------------------------------------------------------------
    # Prozessverwaltung
    # ------------------------------------------------------------------

    @property
    def compose_base(
        self,
    ) -> list[str]:
        return [
            "docker",
            "compose",
            "--env-file",
            str(
                self.repo_root /
                ".env"
            ),
            "-f",
            str(
                self.repo_root /
                "compose" /
                "zero.yml"
            ),
        ]

    def assignment(
        self,
    ) -> list[
        tuple[str, int, int]
    ]:
        """
        Liefert Tupel aus Client-ID, Startindex und Anzahl.
        """

        base = (
            self.total_queries //
            self.clients
        )

        remainder = (
            self.total_queries %
            self.clients
        )

        assignments = []

        start_index = 0

        for i in range(
            self.clients
        ):
            count = (
                base +
                (
                    1
                    if i < remainder
                    else 0
                )
            )

            assignments.append(
                (
                    f"bench-{i}",
                    start_index,
                    count,
                )
            )

            start_index += count

        return assignments

    def container_name(
        self,
        i: int,
    ) -> str:
        return (
            f"{CONTAINER_PREFIX}{i}"
        )

    @staticmethod
    async def sweep_stale_containers(
    ) -> list[str]:
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "ps",
            "-aq",
            "--filter",
            f"name={CONTAINER_PREFIX}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )

        output, _ = (
            await proc.communicate()
        )

        ids = [
            line
            for line
            in output.decode().split()
            if line
        ]

        if not ids:
            return []

        rm = await asyncio.create_subprocess_exec(
            "docker",
            "rm",
            "-f",
            *ids,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )

        await rm.wait()

        return ids

    async def arm(
        self,
    ) -> None:
        """
        Aktiviert den Barrier-Modus vor dem Start des Pools.
        """

        if self._started:
            raise RuntimeError(
                "arm() must be called "
                "before start()."
            )

        self._barrier_mode = True

    async def start(
        self,
    ) -> None:
        if self._started:
            raise RuntimeError(
                "start() was called "
                "more than once."
            )

        stale = (
            await self.sweep_stale_containers()
        )

        if stale:
            self.logs.append(
                f"verwaiste Client-Container "
                f"entfernt: {stale}"
            )

            print(
                f"WARNUNG: {len(stale)} "
                "verwaiste zero-client-Container "
                "gefunden und entfernt."
            )

        self._started = True

        for (
            i,
            (
                client_id,
                start_index,
                count,
            ),
        ) in enumerate(
            self.assignment()
        ):
            cmd = [
                *self.compose_base,
                "run",
                "-T",
                "--rm",
                "--no-deps",
                "--name",
                self.container_name(i),
                "zero-client",
                "npm",
                "run",
                "subscriber",
                "--",
                "--client-id",
                client_id,
                "--queries",
                str(count),
                "--start-index",
                str(start_index),
                "--query-class",
                self.query_class,
                "--topics",
                str(self.topics),
                "--limit",
                str(self.limit),
                "--offset",
                str(self.offset),
                "--needle",
                self.needle,
            ]

            if self.emit_rows:
                cmd.append(
                    "--emit-rows"
                )

            if self._barrier_mode:
                cmd.extend(
                    [
                        "--barrier",
                        "--connect-timeout-ms",
                        "600000",
                    ]
                )

            proc = (
                await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=self.repo_root,
                    stdin=(
                        asyncio.subprocess.PIPE
                        if self._barrier_mode
                        else None
                    ),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
            )

            self.processes.append(
                proc
            )

            self.read_tasks.append(
                asyncio.create_task(
                    self._read_stdout(
                        proc,
                        client_id,
                    )
                )
            )

            self.read_tasks.append(
                asyncio.create_task(
                    self._read_stderr(
                        proc,
                        client_id,
                    )
                )
            )

            self.read_tasks.append(
                asyncio.create_task(
                    self._watch_exit(
                        proc,
                        client_id,
                    )
                )
            )

    async def _read_stdout(
        self,
        proc: asyncio.subprocess.Process,
        client_id: str,
    ) -> None:
        assert (
            proc.stdout
            is not None
        )

        while True:
            line = (
                await proc.stdout.readline()
            )

            if not line:
                break

            text = line.decode(
                "utf-8",
                errors="replace",
            ).strip()

            if not text:
                continue

            try:
                event = json.loads(
                    text
                )

            except json.JSONDecodeError:
                self.logs.append(
                    f"[{client_id}] {text}"
                )

                continue

            self._dispatch(
                event
            )

    async def _read_stderr(
        self,
        proc: asyncio.subprocess.Process,
        client_id: str,
    ) -> None:
        assert (
            proc.stderr
            is not None
        )

        while True:
            line = (
                await proc.stderr.readline()
            )

            if not line:
                break

            text = line.decode(
                "utf-8",
                errors="replace",
            ).strip()

            if text:
                self.logs.append(
                    f"[{client_id}]"
                    f"[stderr] {text}"
                )

    async def _watch_exit(
        self,
        proc: asyncio.subprocess.Process,
        client_id: str,
    ) -> None:
        return_code = (
            await proc.wait()
        )

        if (
            self._closing
            or return_code == 0
        ):
            return

        self._fail(
            {
                "type": "error",
                "client_id":
                    client_id,
                "error":
                    (
                        "client exited "
                        f"with code {return_code}"
                    ),
            }
        )

    # ------------------------------------------------------------------
    # Ereignisverteilung
    # ------------------------------------------------------------------

    def _dispatch(
        self,
        ev: dict[str, Any],
    ) -> None:
        typ = ev.get(
            "type"
        )

        if typ == "probe":
            token = str(
                ev.get(
                    "token"
                )
            )

            future = self._pending.pop(
                token,
                None,
            )

            if (
                future is not None
                and not future.done()
            ):
                future.set_result(
                    ev
                )

            else:
                self._orphans[
                    token
                ] = ev

            return

        if typ == "rows":
            idx = int(
                ev["query_index"]
            )

            self.rows[idx] = [
                int(value)
                for value
                in ev.get(
                    "ids",
                    [],
                )
            ]

            raw_rows = ev.get(
                "rows",
                [],
            )

            if isinstance(
                raw_rows,
                list,
            ):
                self.row_snapshots[
                    idx
                ] = [
                    dict(row)
                    for row
                    in raw_rows
                    if isinstance(
                        row,
                        dict,
                    )
                ]

            self.rows_changed.set()

            return

        if typ == "connected":
            client_id = str(
                ev["client_id"]
            )

            self._connected_clients.add(
                client_id
            )

            if (
                len(
                    self._connected_clients
                )
                >=
                self.clients
            ):
                self._connected_event.set()

            return

        if typ == "armed":
            client_id = str(
                ev["client_id"]
            )

            self._armed_clients.add(
                client_id
            )

            if (
                len(
                    self._armed_clients
                )
                >=
                self.clients
            ):
                self._armed_event.set()

            return

        if typ == "released":
            client_id = str(
                ev["client_id"]
            )

            self._released_clients.add(
                client_id
            )

            return

        if typ == "ready":
            client_id = str(
                ev["client_id"]
            )

            self._ready_clients.add(
                client_id
            )

            if (
                len(
                    self._ready_clients
                )
                >=
                self.clients
            ):
                self._ready_event.set()

            return

        if typ == "progress":
            self.progress[
                str(
                    ev["client_id"]
                )
            ] = int(
                ev.get(
                    "ready",
                    0,
                )
            )

            return

        if typ == "loop-lag":
            client_id = str(
                ev["client_id"]
            )

            lag = float(
                ev.get(
                    "max_lag_ms",
                    0.0,
                )
            )

            if (
                lag >
                self.loop_lag_ms.get(
                    client_id,
                    0.0,
                )
            ):
                self.loop_lag_ms[
                    client_id
                ] = lag

            return

        if typ == "error":
            self._fail(
                ev
            )

            return

        if typ == "started":
            self.logs.append(
                f"[{ev.get('client_id')}] "
                f"started {ev}"
            )

            return

    def _fail(
        self,
        ev: dict[str, Any],
    ) -> None:
        self.errors.append(
            ev
        )

        exception = RuntimeError(
            f"Zero client error: {ev}"
        )

        for future in list(
            self._pending.values()
        ):
            if not future.done():
                future.set_exception(
                    exception
                )

        self._pending.clear()

        self._connected_event.set()
        self._armed_event.set()
        self._ready_event.set()

    # ------------------------------------------------------------------
    # Barrier-Schnittstelle
    # ------------------------------------------------------------------

    async def wait_connected(
        self,
        timeout: float = 600.0,
    ) -> None:
        if not self._barrier_mode:
            raise RuntimeError(
                "wait_connected() requires "
                "barrier mode."
            )

        await asyncio.wait_for(
            self._connected_event.wait(),
            timeout=timeout,
        )

        if self.errors:
            raise RuntimeError(
                f"Zero client error: "
                f"{self.errors[0]}"
            )

        if (
            len(
                self._connected_clients
            )
            < self.clients
        ):
            raise RuntimeError(
                "connected event triggered "
                "before all clients connected."
            )

    async def wait_armed(
        self,
        timeout: float = 600.0,
    ) -> None:
        if not self._barrier_mode:
            raise RuntimeError(
                "wait_armed() requires "
                "barrier mode."
            )

        await asyncio.wait_for(
            self._armed_event.wait(),
            timeout=timeout,
        )

        if self.errors:
            raise RuntimeError(
                f"Zero client error: "
                f"{self.errors[0]}"
            )

        if (
            len(
                self._armed_clients
            )
            < self.clients
        ):
            raise RuntimeError(
                "armed event triggered "
                "before all clients armed."
            )

    async def release(
        self,
    ) -> int:
        if not self._barrier_mode:
            raise RuntimeError(
                "release() requires "
                "barrier mode."
            )

        if not self._started:
            raise RuntimeError(
                "release() called "
                "before start()."
            )

        if self._released:
            raise RuntimeError(
                "release() was already "
                "called."
            )

        if (
            len(
                self._connected_clients
            )
            != self.clients
        ):
            raise RuntimeError(
                "release() called before "
                "all clients connected."
            )

        if (
            len(
                self._armed_clients
            )
            != self.clients
        ):
            raise RuntimeError(
                "release() called before "
                "all clients armed."
            )

        release_epoch_ns = (
            time.time_ns()
        )

        writers = []

        for proc in self.processes:
            if (
                proc.returncode
                is not None
            ):
                raise RuntimeError(
                    "Cannot release barrier: "
                    "a client process already "
                    "exited."
                )

            if proc.stdin is None:
                raise RuntimeError(
                    "Barrier client has "
                    "no stdin pipe."
                )

            proc.stdin.write(
                b"go\n"
            )

            writers.append(
                proc.stdin
            )

        if writers:
            await asyncio.gather(
                *[
                    writer.drain()
                    for writer
                    in writers
                ]
            )

        self._released = True

        self._release_epoch_ns = (
            release_epoch_ns
        )

        return release_epoch_ns

    # ------------------------------------------------------------------
    # Oeffentliche Steady-State-Schnittstelle
    # ------------------------------------------------------------------

    async def wait_ready(
        self,
        timeout: float = 600.0,
    ) -> None:
        await asyncio.wait_for(
            self._ready_event.wait(),
            timeout=timeout,
        )

        if self.errors:
            raise RuntimeError(
                f"Zero client error: "
                f"{self.errors[0]}"
            )

    def register_probe(
        self,
        token: str,
    ) -> asyncio.Future:
        loop = (
            asyncio.get_running_loop()
        )

        future = (
            loop.create_future()
        )

        orphan = self._orphans.pop(
            token,
            None,
        )

        if orphan is not None:
            future.set_result(
                orphan
            )

            return future

        self._pending[
            token
        ] = future

        return future

    async def wait_probe(
        self,
        token: str,
        timeout: float = 30.0,
        future: asyncio.Future | None = None,
    ) -> dict[str, Any] | None:
        selected_future = (
            future
            if future is not None
            else self.register_probe(
                token
            )
        )

        try:
            return await asyncio.wait_for(
                selected_future,
                timeout=timeout,
            )

        except asyncio.TimeoutError:
            self._pending.pop(
                token,
                None,
            )

            self.missed_probes += 1

            return None

    @property
    def max_loop_lag_ms(
        self,
    ) -> float:
        return max(
            self.loop_lag_ms.values(),
            default=0.0,
        )

    def stats(
        self,
    ) -> dict[str, Any]:
        return {
            "total_queries":
                self.total_queries,

            "clients":
                self.clients,

            "query_class":
                self.query_class,

            "offset":
                self.offset,

            "barrier_mode":
                self._barrier_mode,

            "barrier_released":
                self._released,

            "connected_clients":
                sorted(
                    self._connected_clients
                ),

            "armed_clients":
                sorted(
                    self._armed_clients
                ),

            "released_clients":
                sorted(
                    self._released_clients
                ),

            "ready_clients":
                sorted(
                    self._ready_clients
                ),

            "missed_probes":
                self.missed_probes,

            "max_loop_lag_ms":
                self.max_loop_lag_ms,

            "loop_lag_ms_per_client":
                dict(
                    self.loop_lag_ms
                ),

            "errors":
                self.errors[:10],
        }

    async def close(
        self,
    ) -> None:
        self._closing = True

        for proc in self.processes:
            if (
                proc.returncode
                is None
            ):
                proc.terminate()

        for proc in self.processes:
            try:
                await asyncio.wait_for(
                    proc.wait(),
                    timeout=15,
                )

            except asyncio.TimeoutError:
                proc.kill()

                try:
                    await proc.wait()
                except Exception:
                    pass

        for proc in self.processes:
            if proc.stdin is not None:
                try:
                    proc.stdin.close()
                except Exception:
                    pass

        for task in self.read_tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(
            *self.read_tasks,
            return_exceptions=True,
        )

        await self.sweep_stale_containers()
