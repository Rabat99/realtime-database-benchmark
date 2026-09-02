"""
Hasura-Steady-State-Saettigungstest fuer normale Live Queries.

Phasen: Initialisierung, Hintergrundlast, Beruhigungszeit, Messfenster und
Nachlaufzeit.

Standardmaessig verwendet jede Subscription eine eigene Anfrageform mit
eindeutigem Root-Alias. Damit wird die Latenz nicht durch Hasuras Multiplexing
einer gemeinsamen Anfragegruppe bestimmt.

Primaermetrik: write_start_to_observation
Zusatzmetrik: commit_ack_to_observation
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import queue
import random
import threading
import time
import uuid
from pathlib import Path

import psycopg2

from hasura_adapter import HasuraSubscriberPool
from resource_monitor import ResourceMonitor
from zero_workload import (
    WorkloadSpec,
    assert_dataset_matches,
    delete_above,
    insert_rows,
    max_message_id,
    pctl,
)


def load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text().splitlines():
        raw = raw.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        k, v = raw.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def latency_drift(samples: list[dict]) -> dict:
    delivered = [
        s
        for s in sorted(samples, key=lambda x: int(x.get("probe", 0)))
        if s.get("delivered") and s.get("latency_ms") is not None
    ]
    if len(delivered) < 12:
        return {
            "assessable": False,
            "reason": f"nur {len(delivered)} zugestellte Probes",
        }

    n = len(delivered)
    third = n // 3
    first_vals = [float(s["latency_ms"]) for s in delivered[:third]]
    last_vals = [float(s["latency_ms"]) for s in delivered[-third:]]
    first = pctl(first_vals, 50)
    last = pctl(last_vals, 50)
    ratio = (last / first) if first and first > 0 and last is not None else None

    return {
        "assessable": True,
        "first_third_median_ms": first,
        "last_third_median_ms": last,
        "last_to_first_ratio": round(ratio, 3) if ratio is not None else None,
        "first_probe_offset_s": float(delivered[:third][0]["send_offset_s"]),
        "last_probe_offset_s": float(delivered[-third:][-1]["send_offset_s"]),
        "interpretation": "descriptive_only_no_automatic_saturation_threshold",
    }


def build_probe_offsets(*, measure_seconds: float, probes: int, seed: int) -> list[float]:
    """Verteilt die Sonden reproduzierbar ueber das Messfenster."""
    rng = random.Random(seed)
    slot = measure_seconds / probes
    return [(i + rng.uniform(0.15, 0.85)) * slot for i in range(probes)]


def build_probe_indices(*, total_queries: int, probes: int, seed: int) -> list[int]:
    """Verteilt die Sonden reproduzierbar auf die aktiven Anfragen."""
    rng = random.Random(seed ^ 0x5EEDBEEF)

    if probes <= total_queries:
        indices = []
        for i in range(probes):
            lo = (i * total_queries) // probes
            hi = ((i + 1) * total_queries) // probes - 1
            indices.append(rng.randint(lo, max(lo, hi)))
        rng.shuffle(indices)
        return indices

    out = []
    base = list(range(total_queries))
    while len(out) < probes:
        batch = base.copy()
        rng.shuffle(batch)
        out.extend(batch)
    return out[:probes]


def latency_segments(samples: list[dict], *, measure_seconds: float, segments: int) -> list[dict]:
    """Berechnet deskriptive Latenzwerte fuer feste Zeitabschnitte."""
    if segments <= 0:
        return []

    width = measure_seconds / segments
    result = []
    for seg in range(segments):
        start_s = seg * width
        end_s = (seg + 1) * width
        vals = [
            float(s["latency_ms"])
            for s in samples
            if s.get("delivered")
            and s.get("latency_ms") is not None
            and s.get("send_offset_s") is not None
            and start_s <= float(s["send_offset_s"])
            and (
                float(s["send_offset_s"]) < end_s
                or (seg == segments - 1 and float(s["send_offset_s"]) <= end_s)
            )
        ]
        result.append(
            {
                "segment": seg,
                "start_s": round(start_s, 3),
                "end_s": round(end_s, 3),
                "n": len(vals),
                "p50_ms": pctl(vals, 50) if vals else None,
                "p95_ms": pctl(vals, 95) if vals else None,
            }
        )
    return result


class OpenLoopLoadWriter(threading.Thread):
    """
    Open-Loop-Hintergrundlast fuer den Hasura-Saettigungstest.

    Der Scheduler bietet die konfigurierte Rate zeitbasiert an und wartet
    nicht auf den Commit des vorherigen Batches. Mehrere Worker mit jeweils
    eigener PostgreSQL-Verbindung verarbeiten die angebotenen Batches
    parallel. Wenn PostgreSQL die Last nicht mehr annimmt, sinkt nicht die
    angebotene Rate; stattdessen wachsen uncommitted_rows_at_stop und/oder
    die Commit-Latenzen.

    Bei tick_s=50 ms wird pro Takt ein Batch angeboten. So bleibt die Zahl
    der Transaktionen pro Sekunde zwischen den Laststufen vergleichbar.
    """

    def __init__(
        self,
        *,
        connect,
        spec: WorkloadSpec,
        rate_per_s: float,
        hit_ratio: float = 1.0,
        workers: int = 4,
        tick_s: float = 0.05,
        seed: int = 1234,
    ) -> None:
        super().__init__(daemon=True)
        if workers < 1:
            raise ValueError("workers muss >= 1 sein")
        if tick_s <= 0:
            raise ValueError("tick_s muss > 0 sein")

        self.connect = connect
        self.spec = spec
        self.rate_per_s = rate_per_s
        self.hit_ratio = hit_ratio
        self.workers = workers
        self.tick_s = tick_s
        self._rng = random.Random(seed)

        self._stop_requested = threading.Event()
        self._worker_stop = threading.Event()
        self._queue: queue.Queue[list[tuple[int, int, str, int]] | None] = queue.Queue()
        self._lock = threading.Lock()
        self._worker_threads: list[threading.Thread] = []

        self.offered = 0
        self.committed_total = 0
        self.failed_total = 0
        self.errors: list[str] = []

        self._started_at: float | None = None
        self._stopped_at: float | None = None
        self._offered_at_stop = 0
        self._committed_at_stop = 0
        self._failed_at_stop = 0
        self._discarded_queued_rows = 0

    def stop(self) -> None:
        self._stop_requested.set()

    @property
    def elapsed_s(self) -> float:
        if self._started_at is None:
            return 0.0
        end = self._stopped_at if self._stopped_at is not None else time.monotonic()
        return max(end - self._started_at, 0.0)

    @property
    def offered_at_stop(self) -> int:
        if self._stopped_at is None:
            with self._lock:
                return self.offered
        return self._offered_at_stop

    @property
    def written(self) -> int:
        """Bis zum Ende des Lastintervalls erfolgreich committete Zeilen."""
        if self._stopped_at is None:
            with self._lock:
                return self.committed_total
        return self._committed_at_stop

    @property
    def failed_rows(self) -> int:
        if self._stopped_at is None:
            with self._lock:
                return self.failed_total
        return self._failed_at_stop

    @property
    def offered_rate(self) -> float:
        elapsed = self.elapsed_s
        return self.offered_at_stop / elapsed if elapsed > 0 else 0.0

    @property
    def achieved_rate(self) -> float:
        elapsed = self.elapsed_s
        return self.written / elapsed if elapsed > 0 else 0.0

    @property
    def commit_ratio(self) -> float | None:
        offered = self.offered_at_stop
        if offered <= 0:
            return None
        return self.written / offered

    @property
    def uncommitted_rows_at_stop(self) -> int:
        return max(self.offered_at_stop - self.written - self.failed_rows, 0)

    @property
    def discarded_queued_rows(self) -> int:
        return self._discarded_queued_rows

    def _worker(self, worker_id: int) -> None:
        conn = None
        try:
            conn = self.connect()
            conn.autocommit = False

            while not self._worker_stop.is_set():
                try:
                    rows = self._queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                if rows is None:
                    self._queue.task_done()
                    break

                try:
                    insert_rows(conn, rows)
                    with self._lock:
                        self.committed_total += len(rows)
                except Exception as exc:  # noqa: BLE001
                    try:
                        conn.rollback()
                    except Exception:  # noqa: BLE001
                        pass
                    with self._lock:
                        self.failed_total += len(rows)
                        if len(self.errors) < 20:
                            self.errors.append(
                                f"worker={worker_id}: {str(exc)[:500]}"
                            )
                finally:
                    self._queue.task_done()
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass

    def _offer_rows(self, n: int, index: int) -> int:
        if n <= 0:
            return index

        rows = []
        for _ in range(n):
            hit = self._rng.random() < self.hit_ratio
            rows.append(self.spec.row_for(index, hit=hit))
            index = (index + 1) % self.spec.total_queries

        self._queue.put(rows)
        with self._lock:
            self.offered += len(rows)
        return index

    def run(self) -> None:
        if self.rate_per_s <= 0:
            return

        self._started_at = time.monotonic()
        start = self._started_at
        index = 0
        next_tick = start + self.tick_s

        for worker_id in range(self.workers):
            worker = threading.Thread(
                target=self._worker,
                args=(worker_id,),
                daemon=True,
                name=f"hasura-load-writer-{worker_id}",
            )
            self._worker_threads.append(worker)
            worker.start()

        try:
            while not self._stop_requested.is_set():
                now = time.monotonic()
                wait_s = next_tick - now
                if wait_s > 0:
                    self._stop_requested.wait(timeout=wait_s)
                    continue

                elapsed = now - start
                target_offered = int(self.rate_per_s * elapsed)
                with self._lock:
                    already_offered = self.offered
                n = max(target_offered - already_offered, 0)
                index = self._offer_rows(n, index)

                missed_ticks = max(
                    1,
                    int((now - next_tick) // self.tick_s) + 1,
                )
                next_tick += missed_ticks * self.tick_s
        finally:
            self._stopped_at = time.monotonic()
            with self._lock:
                self._offered_at_stop = self.offered
                self._committed_at_stop = self.committed_total
                self._failed_at_stop = self.failed_total

            # Bereits angebotene, aber noch nicht gestartete Batches werden
            # nicht nach dem Lastintervall nachgeschoben. Sie bleiben als
            # Rueckstau in uncommitted_rows_at_stop sichtbar.
            discarded = 0
            while True:
                try:
                    rows = self._queue.get_nowait()
                except queue.Empty:
                    break
                if rows is not None:
                    discarded += len(rows)
                self._queue.task_done()
            self._discarded_queued_rows = discarded

            self._worker_stop.set()
            for _ in self._worker_threads:
                self._queue.put(None)

            for worker in self._worker_threads:
                worker.join(timeout=15.0)
                if worker.is_alive():
                    with self._lock:
                        if len(self.errors) < 20:
                            self.errors.append(
                                f"{worker.name} nach 15s noch aktiv"
                            )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", type=int, default=1)
    ap.add_argument("--clients", type=int, default=2)
    ap.add_argument(
        "--query-class",
        choices=("filter", "sorted", "window", "window_join", "window_search"),
        default="window",
    )
    ap.add_argument("--needle", default="gamma")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--topics", type=int, default=30000)
    ap.add_argument("--users", type=int, default=1000)
    ap.add_argument("--probes", type=int, default=100)
    ap.add_argument("--settle", type=float, default=30.0)
    ap.add_argument("--measure-seconds", type=float, default=60.0)
    ap.add_argument("--drain-seconds", type=float, default=30.0)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--ready-timeout", type=float, default=1200.0)
    ap.add_argument("--registration-rate", type=float, default=200.0)
    ap.add_argument(
        "--query-diversity",
        choices=("unique", "shared"),
        default="unique",
        help=(
            "unique = eigene Query-Shape pro Subscription; "
            "shared = identischer GraphQL-Text, Multiplexing kann greifen"
        ),
    )
    ap.add_argument("--load-rate", type=float, default=0.0)
    ap.add_argument("--hit-ratio", type=float, default=1.0)
    ap.add_argument("--load-writers", type=int, default=4)
    ap.add_argument("--load-tick", type=float, default=0.05)
    ap.add_argument("--harness-cores", type=float, default=8.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--latency-segments", type=int, default=6)
    ap.add_argument("--json-out", default="")
    ap.add_argument("--keep-rows", action="store_true")
    return ap.parse_args()


async def main() -> int:
    args = parse_args()

    if args.queries <= 0:
        raise SystemExit("--queries muss > 0 sein")
    if args.probes <= 0:
        raise SystemExit("--probes muss > 0 sein")
    if args.measure_seconds <= 0:
        raise SystemExit("--measure-seconds muss > 0 sein")
    if args.timeout <= 0:
        raise SystemExit("--timeout muss > 0 sein")
    if args.drain_seconds < args.timeout:
        raise SystemExit("--drain-seconds muss mindestens --timeout entsprechen")
    if args.load_writers <= 0:
        raise SystemExit("--load-writers muss > 0 sein")
    if args.load_tick <= 0:
        raise SystemExit("--load-tick muss > 0 sein")

    repo = Path(__file__).resolve().parents[1]
    env = {**load_dotenv(repo / ".env"), **os.environ}

    def connect():
        return psycopg2.connect(
            host="127.0.0.1",
            port=int(env.get("POSTGRES_PORT", "5432")),
            user=env.get("POSTGRES_USER", "postgres"),
            password=env.get("POSTGRES_PASSWORD", "postgres"),
            dbname=env.get("POSTGRES_DB", "postgres"),
            connect_timeout=5,
        )

    conn = connect()
    conn.autocommit = False

    shape = assert_dataset_matches(conn, topics=args.topics, users=args.users)
    spec = WorkloadSpec(
        query_class=args.query_class,
        total_queries=args.queries,
        topics=args.topics,
        users=args.users,
        needle=args.needle,
    )
    baseline_id = max_message_id(conn)

    hasura_port = env.get("HASURA_PORT", "8080")
    ws_url = f"ws://127.0.0.1:{hasura_port}/v1/graphql"

    pool = HasuraSubscriberPool(
        ws_url=ws_url,
        admin_secret=env.get("HASURA_ADMIN_SECRET", ""),
        total_queries=args.queries,
        clients=args.clients,
        query_class=args.query_class,
        topics=args.topics,
        limit=args.limit,
        needle=args.needle,
        registration_rate=args.registration_rate,
        query_diversity=args.query_diversity,
    )

    monitor = ResourceMonitor(
        repo=repo,
        realtime_prefixes=("compose-hasura-",),
        harness_container_prefixes=(),
        track_replication_lag=False,
    )

    monitor_started = False
    load: OpenLoopLoadWriter | None = None
    resources = None
    probe_samples: list[dict] = []
    stats = {"max_loop_lag_ms": None, "errors": []}

    probe_spacing_s = args.measure_seconds / args.probes
    probe_offsets = build_probe_offsets(
        measure_seconds=args.measure_seconds,
        probes=args.probes,
        seed=args.seed,
    )
    probe_indices = build_probe_indices(
        total_queries=args.queries,
        probes=args.probes,
        seed=args.seed,
    )

    # --load-rate bezeichnet die gesamte angebotene Write-Rate waehrend
    # der Messphase, also Hintergrundwrites plus Probe-Writes.
    probe_offered_rate = args.probes / args.measure_seconds
    if args.load_rate > 0:
        if probe_offered_rate >= args.load_rate:
            raise SystemExit(
                "--load-rate muss groesser als die mittlere Probe-Rate sein "
                f"({probe_offered_rate:.3f}/s)"
            )
        background_load_rate = args.load_rate - probe_offered_rate
    else:
        background_load_rate = 0.0

    actual_measure_s = 0.0
    measure_start = 0.0

    async def wait_one_probe(
        *,
        i: int,
        index: int,
        token: str,
        fut,
        t_write_start_ns: int,
        t_commit_ack_ns: int,
        scheduled_offset_s: float,
        actual_offset_s: float,
    ) -> dict:
        base = {
            "probe": i,
            "query_index_expected": index,
            "scheduled_offset_s": round(scheduled_offset_s, 6),
            "send_offset_s": round(actual_offset_s, 6),
            "schedule_lag_ms": round(
                (actual_offset_s - scheduled_offset_s) * 1000, 3
            ),
            "t_write_start_epoch_ns": t_write_start_ns,
            "t_commit_ack_epoch_ns": t_commit_ack_ns,
            "db_write_commit_ms": (
                t_commit_ack_ns - t_write_start_ns
            ) / 1_000_000,
        }

        try:
            event = await pool.wait_probe(token, timeout=args.timeout, future=fut)
            if event is None:
                return {
                    **base,
                    "query_index_received": None,
                    "delivered": False,
                    "reason": "timeout",
                    "latency_ms": None,
                    "write_to_observation_ms": None,
                    "commit_ack_to_observation_ms": None,
                }

            received_index = int(event["query_index"])
            receive_ns = int(event["t_receive_epoch_ns"])
            e2e_ms = (receive_ns - t_write_start_ns) / 1_000_000
            commit_ms = (receive_ns - t_commit_ack_ns) / 1_000_000

            if args.probes <= 10 or i % 25 == 0:
                print(
                    f"probe {i}: query_index={received_index} "
                    f"e2e_ms={e2e_ms:.3f} commit_ack_ms={commit_ms:.3f}"
                )

            return {
                **base,
                "query_index_received": received_index,
                "delivered": True,
                "reason": None,
                # Einheitliches Kurzfeld fuer die in der Auswertung verwendete
                # Ende-zu-Ende-Latenz ab Beginn des Schreibvorgangs.
                "latency_ms": e2e_ms,
                "write_to_observation_ms": e2e_ms,
                "commit_ack_to_observation_ms": commit_ms,
                "t_receive_epoch_ns": receive_ns,
            }
        except asyncio.CancelledError:
            return {
                **base,
                "query_index_received": None,
                "delivered": False,
                "reason": "drain_expired",
                "latency_ms": None,
                "write_to_observation_ms": None,
                "commit_ack_to_observation_ms": None,
            }

    def write_probe_sync(index: int, token: str) -> tuple[int, int, float]:
        probe_conn = connect()
        probe_conn.autocommit = False
        try:
            write_start_offset_s = time.monotonic() - measure_start
            t_write_start_ns = time.time_ns()
            insert_rows(
                probe_conn,
                [spec.row_for(index, token=token, hit=True)],
            )
            t_commit_ack_ns = time.time_ns()
            return t_write_start_ns, t_commit_ack_ns, write_start_offset_s
        finally:
            probe_conn.close()

    async def run_probe_write_and_wait(
        *,
        i: int,
        index: int,
        token: str,
        fut,
        scheduled_offset_s: float,
        dispatch_offset_s: float,
    ) -> dict:
        t_write_start_ns, t_commit_ack_ns, write_start_offset_s = await asyncio.to_thread(
            write_probe_sync, index, token
        )
        sample = await wait_one_probe(
            i=i,
            index=index,
            token=token,
            fut=fut,
            t_write_start_ns=t_write_start_ns,
            t_commit_ack_ns=t_commit_ack_ns,
            scheduled_offset_s=scheduled_offset_s,
            actual_offset_s=dispatch_offset_s,
        )
        sample["write_start_offset_s"] = round(write_start_offset_s, 6)
        sample["dispatch_to_write_start_ms"] = round(
            (write_start_offset_s - dispatch_offset_s) * 1000,
            3,
        )
        return sample

    try:
        await pool.start()
        print(f"warte auf {args.queries} vollstaendige Hasura-Live-Queries ...")
        await pool.wait_ready(timeout=args.ready_timeout)
        print("READY")
        print(
            "Query-Diversity: "
            f"{args.query_diversity} "
            "(unique = eigene Root-Alias-Shape pro Subscription)"
        )

        if background_load_rate > 0:
            load = OpenLoopLoadWriter(
                connect=connect,
                spec=spec,
                rate_per_s=background_load_rate,
                hit_ratio=args.hit_ratio,
                workers=args.load_writers,
                tick_s=args.load_tick,
            )
            load.start()
            print(
                "Schreiblast waehrend Messphase: "
                f"{background_load_rate:.3f}/s Hintergrund + "
                f"{probe_offered_rate:.3f}/s Probes = "
                f"{args.load_rate:.3f}/s gesamt"
            )
        else:
            print("keine Hintergrundlast: Leerlauflatenz, keine Kapazitaetsaussage")

        if args.settle > 0:
            print(f"settle {args.settle:.0f}s ...")
            await asyncio.sleep(args.settle)

        monitor.start()
        monitor_started = True

        measure_start = time.monotonic()
        measure_end = measure_start + args.measure_seconds
        probe_tasks: list[asyncio.Task] = []

        print(
            f"Messphase {args.measure_seconds:.0f}s, {args.probes} Sonden, "
            f"mittlerer Abstand {probe_spacing_s:.3f}s"
        )

        async def schedule_probe(
            *,
            i: int,
            index: int,
            scheduled_offset_s: float,
        ) -> dict:
            target = measure_start + scheduled_offset_s
            wait_s = target - time.monotonic()
            if wait_s > 0:
                await asyncio.sleep(wait_s)

            dispatch_offset_s = time.monotonic() - measure_start
            token = f"__probe__:{uuid.uuid4()}"
            fut = pool.register_probe(token)
            return await run_probe_write_and_wait(
                i=i,
                index=index,
                token=token,
                fut=fut,
                scheduled_offset_s=scheduled_offset_s,
                dispatch_offset_s=dispatch_offset_s,
            )

        for i, (scheduled_offset_s, index) in enumerate(
            zip(probe_offsets, probe_indices, strict=True)
        ):
            probe_tasks.append(
                asyncio.create_task(
                    schedule_probe(
                        i=i,
                        index=index,
                        scheduled_offset_s=scheduled_offset_s,
                    )
                )
            )

        remaining_measure = measure_end - time.monotonic()
        if remaining_measure > 0:
            await asyncio.sleep(remaining_measure)
        actual_measure_s = time.monotonic() - measure_start

        print(f"Drain {args.drain_seconds:.0f}s ...")
        await asyncio.sleep(args.drain_seconds)

        for task in probe_tasks:
            if not task.done():
                task.cancel()

        raw_samples = await asyncio.gather(*probe_tasks, return_exceptions=True)
        for sample in raw_samples:
            if isinstance(sample, BaseException):
                raise sample
            probe_samples.append(sample)

    finally:
        if load is not None:
            load.stop()
            load.join(timeout=70)
            if load.is_alive():
                load.errors.append(
                    "OpenLoopLoadWriter konnte innerhalb von 70s nicht sauber stoppen"
                )

        if monitor_started:
            monitor.stop()
            monitor.join(timeout=10)
            resources = monitor.report()

        stats = pool.stats()
        await pool.close()

        if not args.keep_rows:
            removed = delete_above(conn, baseline_id)
            print(f"aufgeraeumt: {removed} Zeilen entfernt")

    if resources is None:
        raise RuntimeError("ResourceMonitor hat keinen gueltigen Report geliefert")

    e2e_latencies = [
        float(s["write_to_observation_ms"])
        for s in probe_samples
        if s.get("delivered") and s.get("write_to_observation_ms") is not None
    ]
    commit_latencies = [
        float(s["commit_ack_to_observation_ms"])
        for s in probe_samples
        if s.get("delivered") and s.get("commit_ack_to_observation_ms") is not None
    ]
    write_commit = [
        float(s["db_write_commit_ms"])
        for s in probe_samples
        if s.get("db_write_commit_ms") is not None
    ]

    delivered = len(e2e_latencies)
    misses = args.probes - delivered
    wrong_query = sum(
        1
        for s in probe_samples
        if s.get("delivered")
        and s.get("query_index_received") != s.get("query_index_expected")
    )
    schedule_lags = [
        float(s["schedule_lag_ms"])
        for s in probe_samples
        if s.get("schedule_lag_ms") is not None
    ]

    offered_background = load.offered_rate if load else 0.0
    achieved_background = load.achieved_rate if load else 0.0
    offered_rows = load.offered_at_stop if load else 0
    written = load.written if load else 0
    failed_rows = load.failed_rows if load else 0
    uncommitted_rows = load.uncommitted_rows_at_stop if load else 0
    load_commit_ratio = load.commit_ratio if load else None
    offered_total_est = (
        offered_background + probe_offered_rate if args.load_rate > 0 else None
    )
    achieved_total_est = (
        achieved_background + probe_offered_rate if args.load_rate > 0 else None
    )
    probe_schedule_lag_max_ms = max(schedule_lags) if schedule_lags else None
    schedule_lag_limit_ms = probe_spacing_s * 1000 * 0.5

    valid = {
        "harness_headroom": (
            resources.get("harness_cpu_max_pct") is None
            or resources.get("harness_cpu_max_pct") < 70.0 * args.harness_cores
        ),
        "no_steal": (
            resources.get("steal_max_pct") is None
            or resources.get("steal_max_pct") < 1.0
        ),
        "generator_kept_rate": (
            args.load_rate == 0
            or offered_background >= 0.95 * background_load_rate
        ),
        "probe_schedule_on_time": (
            probe_schedule_lag_max_ms is None
            or probe_schedule_lag_max_ms <= schedule_lag_limit_ms
        ),
        "monitor_ok": not resources.get("monitor_errors", []),
    }

    result = {
        "system": "hasura",
        "query_class": args.query_class,
        "query_diversity": args.query_diversity,
        "query_diversity_strategy": (
            "unique_root_alias_per_subscription"
            if args.query_diversity == "unique"
            else "shared_graphql_text_variables_only"
        ),
        "queries": args.queries,
        "clients": args.clients,
        "registration_rate": args.registration_rate,
        "probes": args.probes,
        "seed": args.seed,
        "probe_schedule": "stratified_jitter",
        "probe_query_sampling": "stratified_full_query_space_shuffled",
        "probe_query_unique": len(set(probe_indices)),
        "probe_query_min": min(probe_indices) if probe_indices else None,
        "probe_query_max": max(probe_indices) if probe_indices else None,
        "dataset": shape,
        "hasura_live_query_refetch_ms": 1000,
        "hasura_live_query_batch_size": 100,
        "latency_definition_primary": "write_start_to_observation",
        "latency_definition_secondary": "commit_ack_to_observation",
        "delivered": delivered,
        "missed": misses,
        "delivery_ratio": round(delivered / args.probes, 4),
        "delivery_complete": misses == 0,
        "wrong_query_index": wrong_query,
        "p50_ms": pctl(e2e_latencies, 50),
        "p95_ms": pctl(e2e_latencies, 95),
        "min_ms": min(e2e_latencies) if e2e_latencies else None,
        "max_ms": max(e2e_latencies) if e2e_latencies else None,
        "p50_commit_ack_ms": pctl(commit_latencies, 50),
        "p95_commit_ack_ms": pctl(commit_latencies, 95),
        "min_commit_ack_ms": min(commit_latencies) if commit_latencies else None,
        "max_commit_ack_ms": max(commit_latencies) if commit_latencies else None,
        "p50_e2e_ms": pctl(e2e_latencies, 50),
        "p95_e2e_ms": pctl(e2e_latencies, 95),
        "min_e2e_ms": min(e2e_latencies) if e2e_latencies else None,
        "max_e2e_ms": max(e2e_latencies) if e2e_latencies else None,
        "negative_commit_ack_samples": sum(1 for x in commit_latencies if x < 0),
        "p50_db_write_commit_ms": pctl(write_commit, 50),
        "p95_db_write_commit_ms": pctl(write_commit, 95),
        "measure_s": round(args.measure_seconds, 1),
        "measure_actual_s": round(actual_measure_s, 3),
        "drain_s": args.drain_seconds,
        "probe_spacing_s": round(probe_spacing_s, 6),
        "probe_schedule_lag_max_ms": probe_schedule_lag_max_ms,
        "probe_schedule_lag_limit_ms": round(schedule_lag_limit_ms, 3),
        "probe_samples": probe_samples,
        "latency_drift": latency_drift(probe_samples),
        "latency_segments": latency_segments(
            probe_samples,
            measure_seconds=args.measure_seconds,
            segments=args.latency_segments,
        ),
        "load_rate": args.load_rate,
        "total_offered_write_rate": args.load_rate,
        "background_load_rate": round(background_load_rate, 3),
        "probe_offered_rate": round(probe_offered_rate, 3),
        "hit_ratio": args.hit_ratio,
        "settle_s": args.settle,
        "load_writer_mode": "open_loop_batched",
        "load_writer_workers": args.load_writers,
        "load_writer_tick_s": args.load_tick,
        "load_offered_rows": offered_rows,
        "load_written": written,
        "load_failed_rows": failed_rows,
        "load_uncommitted_rows_at_stop": uncommitted_rows,
        "load_discarded_queued_rows_after_stop": (
            load.discarded_queued_rows if load else 0
        ),
        "load_elapsed_s": round(load.elapsed_s, 2) if load else 0.0,
        "actual_background_offered_rate": (
            round(offered_background, 3) if load else None
        ),
        "estimated_total_offered_write_rate": (
            round(offered_total_est, 3)
            if offered_total_est is not None
            else None
        ),
        "achieved_background_load_rate": (
            round(achieved_background, 3) if load else None
        ),
        "estimated_total_achieved_write_rate": (
            round(achieved_total_est, 3)
            if achieved_total_est is not None
            else None
        ),
        # Rueckwaertskompatibel fuer hasura_series.py. Dieser Wert beschreibt
        # weiterhin die bis zum Ende des Lastintervalls committete Rate.
        "achieved_load_rate": (
            round(achieved_total_est, 3)
            if achieved_total_est is not None
            else None
        ),
        "load_achieved_ratio": (
            round(achieved_background / background_load_rate, 3)
            if background_load_rate > 0
            else None
        ),
        "load_commit_ratio": (
            round(load_commit_ratio, 4)
            if load_commit_ratio is not None
            else None
        ),
        "load_errors": load.errors[:5] if load else [],
        "max_loop_lag_ms": stats["max_loop_lag_ms"],
        "client_errors": stats["errors"],
        "resources": resources,
        "valid": valid,
    }

    if not all(valid.values()):
        failed = [k for k, v in valid.items() if not v]
        print(f"WARNUNG: Messapparatur ungueltig: {failed}")

    print(json.dumps(result, indent=2))
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(result, indent=2))

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
