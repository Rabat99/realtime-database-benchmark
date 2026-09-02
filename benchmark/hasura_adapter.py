"""
Hasura-Subscriber-Pool fuer normale Live Queries (Poll-and-Diff).

Der Pool unterstuetzt zwei Query-Diversity-Modi:
- shared: alle Subscriptions einer Queryklasse verwenden denselben GraphQL-Text
  und unterscheiden sich nur in Variablen; Hasuras Multiplexing kann greifen.
- unique: jede Subscription erhaelt einen eindeutigen Root-Alias bei ansonsten
  identischer Query-Semantik. Dadurch entstehen unterschiedliche Query-Shapes
  und der Steady-State-Test misst nicht die Latenz einer gemeinsamen
  Multiplex-Gruppe.

Der Pool unterstuetzt zusaetzlich einen optionalen Barrier-Modus fuer
Subscription-Initialisierungsmessungen:
1. alle WebSocket-Verbindungen aufbauen,
2. auf connection_ack warten,
3. alle Clients an der Barriere sammeln,
4. gemeinsam freigeben,
5. erst dann die gemessenen Subscriptions starten.

Ohne arm() arbeitet der Pool im Steady-State-Modus.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import websockets


def hasura_query(query_class: str, *, root_alias: str | None = None) -> str:
    base_fields = "id topic_id user_id content created_at"
    root_field = f"{root_alias}: messages" if root_alias else "messages"

    if query_class == "filter":
        return f"""
        subscription($topicId: bigint!) {{
          {root_field}(where: {{topic_id: {{_eq: $topicId}}}}) {{
            {base_fields}
          }}
        }}
        """

    if query_class == "sorted":
        return f"""
        subscription($topicId: bigint!) {{
          {root_field}(
            where: {{topic_id: {{_eq: $topicId}}}},
            order_by: [{{created_at: desc}}, {{id: desc}}]
          ) {{
            {base_fields}
          }}
        }}
        """

    if query_class == "window":
        return f"""
        subscription($topicId: bigint!, $limit: Int!) {{
          {root_field}(
            where: {{topic_id: {{_eq: $topicId}}}},
            order_by: [{{created_at: desc}}, {{id: desc}}],
            limit: $limit
          ) {{
            {base_fields}
          }}
        }}
        """

    if query_class == "window_join":
        return f"""
        subscription($topicId: bigint!, $limit: Int!) {{
          {root_field}(
            where: {{topic_id: {{_eq: $topicId}}}},
            order_by: [{{created_at: desc}}, {{id: desc}}],
            limit: $limit
          ) {{
            {base_fields}
            user {{ id name }}
          }}
        }}
        """

    if query_class == "window_search":
        return f"""
        subscription($topicId: bigint!, $limit: Int!, $pattern: String!) {{
          {root_field}(
            where: {{
              topic_id: {{_eq: $topicId}},
              content: {{_ilike: $pattern}}
            }},
            order_by: [{{created_at: desc}}, {{id: desc}}],
            limit: $limit
          ) {{
            {base_fields}
          }}
        }}
        """

    raise ValueError(f"unbekannte query_class: {query_class}")


class HasuraSubscriberPool:
    def __init__(
        self,
        *,
        ws_url: str,
        admin_secret: str,
        total_queries: int,
        clients: int = 2,
        query_class: str = "window",
        topics: int = 30000,
        limit: int = 10,
        needle: str = "gamma",
        registration_rate: float = 200.0,
        emit_rows: bool = False,
        query_diversity: str = "shared",
    ) -> None:
        if total_queries < 1:
            raise ValueError("total_queries must be >= 1")
        if clients < 1:
            raise ValueError("clients must be >= 1")
        if clients > total_queries:
            clients = total_queries
        if total_queries > topics:
            raise ValueError("mehr Queries als Topics")
        if query_diversity not in ("shared", "unique"):
            raise ValueError("query_diversity muss 'shared' oder 'unique' sein")

        self.ws_url = ws_url
        self.admin_secret = admin_secret
        self.total_queries = total_queries
        self.clients = clients
        self.query_class = query_class
        self.topics = topics
        self.limit = limit
        self.needle = needle
        self.registration_rate = registration_rate
        self.emit_rows = emit_rows
        self.query_diversity = query_diversity

        # Fuer Rueckwaertskompatibilitaet bleibt der gemeinsame Query-Text
        # verfuegbar. Im unique-Modus wird pro Subscription ein eigener Text
        # mit eindeutigem Root-Alias erzeugt.
        self.query_text = hasura_query(query_class)

        self._ready_queries: set[int] = set()
        self._ready_event = asyncio.Event()
        self._pending: dict[str, asyncio.Future] = {}
        self._orphans: dict[str, dict[str, Any]] = {}
        self._highest_seen_id: dict[int, int] = {}
        self._tasks: list[asyncio.Task] = []
        self._websockets: list[Any] = []
        self._closing = False

        self.errors: list[dict[str, Any]] = []
        self.missed_probes = 0
        self.row_snapshots: dict[int, list[dict[str, Any]]] = {}
        self.rows_changed = asyncio.Event()

        self._loop_lag_max_ms = 0.0
        self._loop_lag_task: asyncio.Task | None = None
        self._loop_lag_measurement_enabled = True

        self._barrier_enabled = False
        self._release_event = asyncio.Event()

        self._connected_clients: set[str] = set()
        self._connected_event = asyncio.Event()

        self._armed_clients: set[str] = set()
        self._armed_event = asyncio.Event()

        self._released_clients: set[str] = set()

    def assignment(self) -> list[tuple[str, int, int]]:
        base = self.total_queries // self.clients
        rem = self.total_queries % self.clients
        out = []
        start = 0

        for i in range(self.clients):
            count = base + (1 if i < rem else 0)
            out.append((f"hasura-{i}", start, count))
            start += count

        return out

    def response_field_for(self, global_index: int) -> str:
        if self.query_diversity == "unique":
            return f"q_{global_index}"
        return "messages"

    def query_text_for(self, global_index: int) -> str:
        if self.query_diversity == "unique":
            return hasura_query(
                self.query_class,
                root_alias=self.response_field_for(global_index),
            )
        return self.query_text

    def variables_for(self, global_index: int) -> dict[str, Any]:
        v: dict[str, Any] = {"topicId": global_index + 1}

        if self.query_class in (
            "window",
            "window_join",
            "window_search",
        ):
            v["limit"] = self.limit

        if self.query_class == "window_search":
            v["pattern"] = f"%{self.needle}%"

        return v

    def _dispatch(self, ev: dict[str, Any]) -> None:
        typ = ev.get("type")
        client_id = str(ev.get("client_id", ""))

        if typ == "connected":
            self._connected_clients.add(client_id)
            if len(self._connected_clients) >= self.clients:
                self._connected_event.set()

        elif typ == "armed":
            self._armed_clients.add(client_id)
            if len(self._armed_clients) >= self.clients:
                self._armed_event.set()

        elif typ == "released":
            self._released_clients.add(client_id)

    async def _loop_lag_monitor(self) -> None:
        tick = 0.1
        previous = time.monotonic()

        while not self._closing:
            await asyncio.sleep(tick)
            now = time.monotonic()
            elapsed = now - previous
            previous = now

            if not self._loop_lag_measurement_enabled:
                continue

            lag_ms = max(0.0, (elapsed - tick) * 1000.0)

            if lag_ms > self._loop_lag_max_ms:
                self._loop_lag_max_ms = lag_ms

    async def arm(self) -> None:
        if self._tasks:
            raise RuntimeError("arm() must be called before start().")

        self._barrier_enabled = True
        self._loop_lag_measurement_enabled = False
        self._loop_lag_max_ms = 0.0

    async def start(self) -> None:
        self._loop_lag_task = asyncio.create_task(
            self._loop_lag_monitor()
        )

        for client_id, start, count in self.assignment():
            self._tasks.append(
                asyncio.create_task(
                    self._run_client(client_id, start, count)
                )
            )

    async def wait_connected(self, timeout: float = 600.0) -> None:
        await asyncio.wait_for(
            self._connected_event.wait(),
            timeout=timeout,
        )

        if self.errors:
            raise RuntimeError(f"Hasura client error: {self.errors[0]}")

        if len(self._connected_clients) != self.clients:
            raise RuntimeError(
                f"nur {len(self._connected_clients)}/{self.clients} Clients connected"
            )

    async def wait_armed(self, timeout: float = 600.0) -> None:
        if not self._barrier_enabled:
            raise RuntimeError(
                "wait_armed() requires arm() before start()."
            )

        await asyncio.wait_for(
            self._armed_event.wait(),
            timeout=timeout,
        )

        if self.errors:
            raise RuntimeError(f"Hasura client error: {self.errors[0]}")

        if len(self._armed_clients) != self.clients:
            raise RuntimeError(
                f"nur {len(self._armed_clients)}/{self.clients} Clients armed"
            )

    async def release(self) -> int:
        if not self._barrier_enabled:
            raise RuntimeError(
                "release() requires arm() before start()."
            )

        if len(self._connected_clients) != self.clients:
            raise RuntimeError(
                "release() before all clients are connected"
            )

        if len(self._armed_clients) != self.clients:
            raise RuntimeError(
                "release() before all clients are armed"
            )

        release_ns = time.time_ns()

        self._loop_lag_max_ms = 0.0
        self._loop_lag_measurement_enabled = True
        self._release_event.set()

        return release_ns

    async def _run_client(
        self,
        client_id: str,
        start: int,
        count: int,
    ) -> None:
        headers = {}

        if self.admin_secret:
            headers["x-hasura-admin-secret"] = self.admin_secret

        ws = None

        try:
            ws = await websockets.connect(
                self.ws_url,
                subprotocols=["graphql-ws"],
                open_timeout=15,
                additional_headers=headers,
                max_size=None,
            )

            self._websockets.append(ws)

            await ws.send(
                json.dumps(
                    {
                        "type": "connection_init",
                        "payload": {"headers": headers},
                    }
                )
            )

            while True:
                msg = json.loads(
                    await asyncio.wait_for(
                        ws.recv(),
                        timeout=30,
                    )
                )

                if msg.get("type") == "connection_ack":
                    break

                if msg.get("type") in (
                    "connection_error",
                    "error",
                ):
                    raise RuntimeError(
                        f"Hasura connection error: {msg}"
                    )

            self._dispatch(
                {
                    "type": "connected",
                    "client_id": client_id,
                    "t_epoch_ns": time.time_ns(),
                }
            )

            reader = asyncio.create_task(
                self._reader(ws, client_id)
            )

            if self._barrier_enabled:
                self._dispatch(
                    {
                        "type": "armed",
                        "client_id": client_id,
                        "t_epoch_ns": time.time_ns(),
                    }
                )

                await self._release_event.wait()

                if self._closing:
                    reader.cancel()
                    await asyncio.gather(
                        reader,
                        return_exceptions=True,
                    )
                    return

                self._dispatch(
                    {
                        "type": "released",
                        "client_id": client_id,
                        "t_epoch_ns": time.time_ns(),
                    }
                )

            delay = (
                self.clients / self.registration_rate
                if self.registration_rate > 0
                else 0.0
            )

            for local in range(count):
                if self._closing:
                    break

                global_index = start + local
                sid = str(global_index)

                self._dispatch(
                    {
                        "type": "query-start",
                        "client_id": client_id,
                        "query_index": global_index,
                        "t_epoch_ns": time.time_ns(),
                    }
                )

                await ws.send(
                    json.dumps(
                        {
                            "id": sid,
                            "type": "start",
                            "payload": {
                                "query": self.query_text_for(global_index),
                                "variables": self.variables_for(
                                    global_index
                                ),
                            },
                        }
                    )
                )

                if delay > 0:
                    await asyncio.sleep(delay)

            await reader

        except asyncio.CancelledError:
            pass

        except Exception as exc:  # noqa: BLE001
            if not self._closing:
                self._fail(
                    {
                        "type": "error",
                        "client_id": client_id,
                        "t_epoch_ns": time.time_ns(),
                        "error": str(exc)[:500],
                    }
                )

        finally:
            if ws is not None:
                try:
                    await ws.close()
                except Exception:  # noqa: BLE001
                    pass

    async def _reader(self, ws, client_id: str) -> None:
        while not self._closing:
            try:
                raw = await asyncio.wait_for(
                    ws.recv(),
                    timeout=0.5,
                )
            except asyncio.TimeoutError:
                continue

            msg = json.loads(raw)
            typ = msg.get("type")

            if typ in ("data", "next"):
                sid = msg.get("id")

                if sid is None:
                    continue

                global_index = int(sid)
                payload = msg.get("payload") or {}

                if payload.get("errors"):
                    self._fail(
                        {
                            "type": "error",
                            "client_id": client_id,
                            "query_index": global_index,
                            "t_epoch_ns": time.time_ns(),
                            "error": str(
                                payload.get("errors")
                            )[:500],
                        }
                    )
                    continue

                data = payload.get("data") or {}
                response_field = self.response_field_for(global_index)
                rows = data.get(response_field)

                if not isinstance(rows, list):
                    self._fail(
                        {
                            "type": "error",
                            "client_id": client_id,
                            "query_index": global_index,
                            "t_epoch_ns": time.time_ns(),
                            "error": (
                                f"{response_field} ist kein Array: "
                                f"{str(data)[:300]}"
                            ),
                        }
                    )
                    continue

                if global_index not in self._ready_queries:
                    self._ready_queries.add(global_index)

                    self._dispatch(
                        {
                            "type": "query-ready",
                            "client_id": client_id,
                            "query_index": global_index,
                            "t_epoch_ns": time.time_ns(),
                        }
                    )

                    if len(self._ready_queries) >= self.total_queries:
                        self._ready_event.set()

                self._scan_rows(global_index, rows)

                if self.emit_rows:
                    self.row_snapshots[global_index] = [
                        dict(r)
                        for r in rows
                    ]
                    self.rows_changed.set()

            elif typ == "error":
                self._fail(
                    {
                        "type": "error",
                        "client_id": client_id,
                        "t_epoch_ns": time.time_ns(),
                        "error": str(
                            msg.get("payload")
                        )[:500],
                    }
                )

    def _scan_rows(
        self,
        global_index: int,
        rows: list[dict[str, Any]],
    ) -> None:
        floor = self._highest_seen_id.get(global_index, 0)
        max_id = floor

        for row in rows:
            try:
                mid = int(row.get("id", 0))
            except Exception:  # noqa: BLE001
                continue

            if mid > max_id:
                max_id = mid

            if mid <= floor:
                continue

            content = str(row.get("content", ""))
            p = content.find("__probe__:")

            if p < 0:
                continue

            token = content[p:].split()[0]

            event = {
                "type": "probe",
                "query_index": global_index,
                "token": token,
                "t_receive_epoch_ns": time.time_ns(),
            }

            fut = self._pending.pop(token, None)

            if fut is not None and not fut.done():
                fut.set_result(event)
            else:
                self._orphans[token] = event

        self._highest_seen_id[global_index] = max_id

    def _fail(self, ev: dict[str, Any]) -> None:
        self.errors.append(ev)

        exc = RuntimeError(
            f"Hasura client error: {ev}"
        )

        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(exc)

        self._pending.clear()

        self._connected_event.set()
        self._armed_event.set()
        self._ready_event.set()

    async def wait_ready(self, timeout: float = 900.0) -> None:
        await asyncio.wait_for(
            self._ready_event.wait(),
            timeout=timeout,
        )

        if self.errors:
            raise RuntimeError(
                f"Hasura client error: {self.errors[0]}"
            )

        if len(self._ready_queries) != self.total_queries:
            raise RuntimeError(
                f"nur {len(self._ready_queries)}/{self.total_queries} Queries ready"
            )

    def register_probe(self, token: str) -> asyncio.Future:
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        orphan = self._orphans.pop(token, None)

        if orphan is not None:
            fut.set_result(orphan)
            return fut

        self._pending[token] = fut
        return fut

    async def wait_probe(
        self,
        token: str,
        timeout: float = 30.0,
        future: asyncio.Future | None = None,
    ) -> dict[str, Any] | None:
        fut = (
            future
            if future is not None
            else self.register_probe(token)
        )

        try:
            return await asyncio.wait_for(
                fut,
                timeout=timeout,
            )

        except asyncio.TimeoutError:
            self._pending.pop(token, None)
            self.missed_probes += 1
            return None

    @property
    def max_loop_lag_ms(self) -> float:
        return self._loop_lag_max_ms

    def stats(self) -> dict[str, Any]:
        return {
            "total_queries": self.total_queries,
            "clients": self.clients,
            "query_class": self.query_class,
            "query_diversity": self.query_diversity,
            "ready_queries": len(self._ready_queries),
            "connected_clients": len(self._connected_clients),
            "armed_clients": len(self._armed_clients),
            "released_clients": len(self._released_clients),
            "missed_probes": self.missed_probes,
            "max_loop_lag_ms": round(
                self.max_loop_lag_ms,
                3,
            ),
            "errors": self.errors[:10],
        }

    async def close(self) -> None:
        self._closing = True
        self._release_event.set()

        for task in self._tasks:
            if not task.done():
                task.cancel()

        await asyncio.gather(
            *self._tasks,
            return_exceptions=True,
        )

        self._tasks.clear()

        if self._loop_lag_task is not None:
            if not self._loop_lag_task.done():
                self._loop_lag_task.cancel()

            await asyncio.gather(
                self._loop_lag_task,
                return_exceptions=True,
            )

        self._websockets.clear()
