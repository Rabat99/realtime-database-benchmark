# -*- coding: utf-8 -*-
"""
Kontrollierter Zero-Steady-State-Latenzlauf.

Methodische Eigenschaften:
* Subscriptions werden vor der Messung vollstaendig initialisiert (READY).
* Danach folgt ein Settle-Fenster; Hydration ist nicht Teil der Steady-State-
  Messung.
* Feste Messdauer statt einer von der Antwortlatenz abhaengigen Laufzeit.
* Die Sondenschreibvorgaenge werden gleichmaessig ueber die Messphase verteilt.
* Sondenergebnisse werden parallel erwartet; eine langsame Sonde blockiert die
  naechste Sonde nicht.
* Die Hintergrundschreiblast laeuft waehrend Beruhigungszeit, Messphase und
  Nachlaufzeit.
* Eine feste Drain-Phase gibt auch der letzten Probe das volle Timeout-Fenster.
* Apparaturgueltigkeit (Harness, Generator, Steal, Sondenplanung, Monitor)
  wird getrennt vom Systemergebnis (Delivery/Latenz) ausgewiesen.

Latenzdefinitionen:
1. Primaermetrik: write_start_to_observation
   Zeitstempel unmittelbar VOR dem INSERT bis zur Beobachtung am Subscriber.
   Diese Ende-zu-Ende-Definition entspricht dem Messprinzip der InvaliDB-
   Evaluation und umfasst DB-Write/Commit + Realtime-Propagation.
2. Zusatzmetrik: commit_ack_to_observation
   Rueckkehr von insert_rows() (nach conn.commit()) bis zur Beobachtung.
   Dieser Wert kann theoretisch negativ sein, wenn die Subscription die
   Aenderung vor der Commit-Bestaetigung am Writer beobachtet. Solche Werte
   werden nicht auf 0 gekappt.
3. db_write_commit_ms
   Dauer vom Write-Start bis zur Rueckkehr der Commit-Bestaetigung.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import subprocess
import time
import uuid
from pathlib import Path

import psycopg2

from zero_adapter import ZeroSubscriberPool
from zero_monitor import ResourceMonitor
from zero_workload import (
    LoadWriter,
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


def zero_state_sizes(env: dict[str, str], repo: Path) -> dict[str, object]:
    out: dict[str, object] = {"cvr_bytes": None, "replica_bytes": None}

    try:
        meta = psycopg2.connect(
            host="127.0.0.1",
            port=int(env.get("POSTGRES_META_PORT", "5433")),
            user=env.get("POSTGRES_USER", "postgres"),
            password=env.get("POSTGRES_PASSWORD", "postgres"),
            dbname="zero_cvr",
        )
        with meta, meta.cursor() as cur:
            cur.execute("SELECT pg_database_size('zero_cvr')")
            out["cvr_bytes"] = int(cur.fetchone()[0])
        meta.close()
    except Exception as exc:  # noqa: BLE001
        out["cvr_bytes"] = f"unavailable: {exc}"

    try:
        res = subprocess.run(
            [
                "docker",
                "compose",
                "--env-file",
                str(repo / ".env"),
                "-f",
                str(repo / "compose" / "zero.yml"),
                "exec",
                "-T",
                "zero-cache",
                "stat",
                "-c",
                "%s",
                "/data/replica.db",
            ],
            cwd=repo,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if res.returncode == 0:
            out["replica_bytes"] = int(res.stdout.strip())
    except Exception:  # noqa: BLE001
        pass

    return out


def latency_drift(samples: list[dict]) -> dict:
    """Berechnet den deskriptiven Trend ohne feste Saettigungsschwelle."""
    delivered = [
        s for s in sorted(samples, key=lambda x: int(x.get("probe", 0)))
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

    first_t = float(delivered[:third][0]["send_offset_s"])
    last_t = float(delivered[-third:][-1]["send_offset_s"])

    return {
        "assessable": True,
        "first_third_median_ms": first,
        "last_third_median_ms": last,
        "last_to_first_ratio": round(ratio, 3) if ratio is not None else None,
        "first_probe_offset_s": first_t,
        "last_probe_offset_s": last_t,
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


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", type=int, default=1)
    ap.add_argument("--clients", type=int, default=1)
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
    ap.add_argument("--ready-timeout", type=float, default=900.0)
    ap.add_argument("--load-rate", type=float, default=0.0)
    ap.add_argument("--hit-ratio", type=float, default=1.0)
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
        raise SystemExit(
            "--drain-seconds muss mindestens so gross wie --timeout sein, "
            "damit die letzte Probe ihr volles Timeout-Fenster erhaelt"
        )

    repo = Path(__file__).resolve().parents[1]
    env = {**load_dotenv(repo / ".env"), **os.environ}

    def connect():
        return psycopg2.connect(
            host="127.0.0.1",
            port=int(env.get("POSTGRES_PORT", "5432")),
            user=env.get("POSTGRES_USER", "postgres"),
            password=env.get("POSTGRES_PASSWORD", "postgres"),
            dbname=env.get("POSTGRES_DB", "postgres"),
        )

    conn = connect()
    conn.autocommit = False

    shape = assert_dataset_matches(conn, topics=args.topics, users=args.users)
    state_sizes = zero_state_sizes(env, repo)
    print(f"Zero-Zustand vor dem Lauf: {state_sizes}")

    spec = WorkloadSpec(
        query_class=args.query_class,
        total_queries=args.queries,
        topics=args.topics,
        users=args.users,
        needle=args.needle,
    )

    baseline_id = max_message_id(conn)

    pool = ZeroSubscriberPool(
        repo_root=repo,
        total_queries=args.queries,
        clients=args.clients,
        query_class=args.query_class,
        topics=args.topics,
        limit=args.limit,
        needle=args.needle,
    )

    monitor = ResourceMonitor(
        repo=repo,
        compose_file=repo / "compose" / "zero.yml",
        env_file=repo / ".env",
    )

    monitor_started = False
    load: LoadWriter | None = None
    probe_samples: list[dict] = []
    resources = None
    stats = {
        "loop_lag_ms_per_client": {},
        "max_loop_lag_ms": None,
        "errors": [],
    }

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

    # --load-rate bezeichnet alle waehrend der Messphase angebotenen Writes.
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
        try:
            event = await pool.wait_probe(token, timeout=args.timeout, future=fut)
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

            if "query_index" not in event:
                raise RuntimeError(
                    "Sonde ohne query_index empfangen. zero-client Image neu bauen."
                )

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
                # latency_ms bleibt der Kurzname fuer die Primaermetrik.
                "latency_ms": e2e_ms,
                "write_to_observation_ms": e2e_ms,
                "commit_ack_to_observation_ms": commit_ms,
                "t_receive_epoch_ns": receive_ns,
            }

        except asyncio.CancelledError:
            return {
                "probe": i,
                "query_index_expected": index,
                "query_index_received": None,
                "delivered": False,
                "reason": "drain_expired",
                "latency_ms": None,
                "write_to_observation_ms": None,
                "commit_ack_to_observation_ms": None,
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


    async def run_one_probe(
        *,
        i: int,
        index: int,
        scheduled_offset_s: float,
        measure_start: float,
    ) -> dict:
        """Fuehrt eine Sonde unabhaengig von den uebrigen Sonden aus.

        INSERT und COMMIT laufen in einem Arbeitsthread. Ein langsamer Commit
        blockiert dadurch weder die Ereignisschleife noch die Planung der
        folgenden Sonden.
        """
        target = measure_start + scheduled_offset_s
        wait_s = target - time.monotonic()
        if wait_s > 0:
            await asyncio.sleep(wait_s)

        token = f"__probe__:{uuid.uuid4()}"

        # Die Registrierung erfolgt vor dem Write, damit kein Ereignis verloren geht.
        fut = pool.register_probe(token)

        def write_probe_sync() -> tuple[float, int, int]:
            # Jeder Arbeitsthread verwendet eine eigene PostgreSQL-Verbindung.
            probe_conn = connect()
            probe_conn.autocommit = False
            try:
                # Der Verbindungsaufbau liegt ausserhalb der Primaermetrik.
                # Seine Dauer bleibt ueber schedule_lag_ms erkennbar.
                actual_offset_s = time.monotonic() - measure_start

                t_write_start_ns = time.time_ns()
                insert_rows(
                    probe_conn,
                    [spec.row_for(index, token=token, hit=True)],
                )
                t_commit_ack_ns = time.time_ns()

                return actual_offset_s, t_write_start_ns, t_commit_ack_ns
            finally:
                probe_conn.close()

        (
            actual_offset_s,
            t_write_start_ns,
            t_commit_ack_ns,
        ) = await asyncio.to_thread(write_probe_sync)

        return await wait_one_probe(
            i=i,
            index=index,
            token=token,
            fut=fut,
            t_write_start_ns=t_write_start_ns,
            t_commit_ack_ns=t_commit_ack_ns,
            scheduled_offset_s=scheduled_offset_s,
            actual_offset_s=actual_offset_s,
        )

    try:
        await pool.start()
        print(f"warte auf {args.queries} vollstaendige Zero-Queries ...")
        await pool.wait_ready(timeout=args.ready_timeout)
        print("READY")

        if background_load_rate > 0:
            load = LoadWriter(
                connect=connect,
                spec=spec,
                rate_per_s=background_load_rate,
                hit_ratio=args.hit_ratio,
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
            f"Abstand {probe_spacing_s:.3f}s"
        )

        for i, (scheduled_offset_s, index) in enumerate(
            zip(probe_offsets, probe_indices, strict=True)
        ):
            probe_tasks.append(
                asyncio.create_task(
                    run_one_probe(
                        i=i,
                        index=index,
                        scheduled_offset_s=scheduled_offset_s,
                        measure_start=measure_start,
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
            load.join(timeout=10)

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

    written = load.written if load else 0
    achieved_background = load.achieved_rate if load else 0.0
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
            or achieved_background >= 0.95 * background_load_rate
        ),
        "probe_schedule_on_time": (
            probe_schedule_lag_max_ms is None
            or probe_schedule_lag_max_ms <= schedule_lag_limit_ms
        ),
        "monitor_ok": not resources.get("monitor_errors", []),
    }

    result = {
        "query_class": args.query_class,
        "queries": args.queries,
        "clients": args.clients,
        "probes": args.probes,
        "seed": args.seed,
        "probe_schedule": "stratified_jitter",
        "probe_query_sampling": "stratified_full_query_space_shuffled",
        "probe_query_unique": len(set(probe_indices)),
        "probe_query_min": min(probe_indices) if probe_indices else None,
        "probe_query_max": max(probe_indices) if probe_indices else None,
        "dataset": shape,
        "zero_state": state_sizes,
        "latency_definition_primary": "write_start_to_observation",
        "latency_definition_secondary": "commit_ack_to_observation",
        "delivered": delivered,
        "missed": misses,
        "delivery_ratio": round(delivered / args.probes, 4),
        "delivery_complete": misses == 0,
        "wrong_query_index": wrong_query,
        # Primaermetrik: Beginn des Schreibvorgangs bis zur Beobachtung.
        "p50_ms": pctl(e2e_latencies, 50),
        "p95_ms": pctl(e2e_latencies, 95),
        "min_ms": min(e2e_latencies) if e2e_latencies else None,
        "max_ms": max(e2e_latencies) if e2e_latencies else None,
        # Zusaetzliche Sicht ab Commit-Bestaetigung.
        "p50_commit_ack_ms": pctl(commit_latencies, 50),
        "p95_commit_ack_ms": pctl(commit_latencies, 95),
        "min_commit_ack_ms": min(commit_latencies) if commit_latencies else None,
        "max_commit_ack_ms": max(commit_latencies) if commit_latencies else None,
        # Explizite Ende-zu-Ende-Felder fuer die Primaermetrik.
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
        "load_written": written,
        "load_elapsed_s": round(load.elapsed_s, 2) if load else 0.0,
        "achieved_background_load_rate": (
            round(achieved_background, 3) if load else None
        ),
        "estimated_total_achieved_write_rate": (
            round(achieved_total_est, 3)
            if achieved_total_est is not None
            else None
        ),
        # Rueckwaertskompatibel fuer zero_series.py: Gesamt-Write-Rate.
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
        "load_errors": load.errors[:5] if load else [],
        "loop_lag_reported": bool(stats["loop_lag_ms_per_client"]),
        "max_loop_lag_ms": stats["max_loop_lag_ms"],
        "loop_lag_ms_per_client": stats["loop_lag_ms_per_client"],
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
        # Zustellverluste sind Systemergebnisse. Nur technische Ausnahmen
        # beenden den Lauf mit einem Fehlercode.
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
