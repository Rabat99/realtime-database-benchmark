"""
Kontrollierte Hasura-Messserie fuer normale Live Queries.

query: Schreibrate konstant, Queryzahl variieren.
write: Queryzahl konstant, Schreibrate variieren.

Kein automatischer Latenz-/SLA-Cutoff. Apparaturfehler werden begrenzt
wiederholt und separat protokolliert. Nur apparativ gueltige Runs gehen in die
Stufenmediane ein.
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BENCH_DIR = Path(__file__).resolve().parent


def compose(*args: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--env-file",
        str(REPO / ".env"),
        "-f",
        str(REPO / "compose" / "hasura.yml"),
        *args,
    ]


def prepare_run(args) -> None:
    print("  Hasura neu starten ...")
    subprocess.run(compose("restart", "hasura"), check=True, text=True)
    print(f"  warte {args.restart_wait:.0f}s ...")
    time.sleep(args.restart_wait)

    print("  Preflight ...")
    proc = subprocess.run(
        [
            sys.executable,
            str(BENCH_DIR / "hasura_preflight.py"),
            "--expect-messages",
            str(args.expect_messages),
            "--expect-topics",
            str(args.topics),
            "--expect-users",
            str(args.users),
        ],
        cwd=BENCH_DIR,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Hasura-Preflight fehlgeschlagen: {proc.returncode}")


def apparatus_valid(result: dict) -> tuple[bool, list[str]]:
    valid = result.get("valid", {})
    required = (
        "harness_headroom",
        "no_steal",
        "generator_kept_rate",
        "probe_schedule_on_time",
        "monitor_ok",
    )
    bad = [key for key in required if not valid.get(key, False)]
    return len(bad) == 0, bad


def run_one(
    args,
    *,
    queries: int,
    load_rate: float,
    repeat: int,
    attempt: int,
    stage_label: str,
    out_dir: Path,
) -> dict:
    safe_rate = str(load_rate).replace(".", "p")
    out_file = out_dir / (
        f"{stage_label}_q{queries}_r{safe_rate}_run{repeat}_attempt{attempt}.json"
    )

    cmd = [
        sys.executable,
        str(BENCH_DIR / "hasura_smoke.py"),
        "--queries",
        str(queries),
        "--clients",
        str(args.clients),
        "--query-class",
        args.query_class,
        "--needle",
        args.needle,
        "--limit",
        str(args.limit),
        "--topics",
        str(args.topics),
        "--users",
        str(args.users),
        "--probes",
        str(args.probes),
        "--settle",
        str(args.settle),
        "--measure-seconds",
        str(args.measure_seconds),
        "--drain-seconds",
        str(args.drain_seconds),
        "--timeout",
        str(args.timeout),
        "--ready-timeout",
        str(args.ready_timeout),
        "--registration-rate",
        str(args.registration_rate),
        "--load-rate",
        str(load_rate),
        "--hit-ratio",
        str(args.hit_ratio),
        "--harness-cores",
        str(args.harness_cores),
        "--json-out",
        str(out_file),
    ]

    proc = subprocess.run(cmd, cwd=BENCH_DIR, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"hasura_smoke.py fehlgeschlagen: Q={queries}, Rate={load_rate}, "
            f"Repeat={repeat}, Attempt={attempt}, Code={proc.returncode}"
        )
    if not out_file.exists():
        raise RuntimeError(f"Ergebnisdatei fehlt: {out_file}")

    result = json.loads(out_file.read_text())
    result["raw_file"] = str(out_file)
    return result


def median_or_none(values: list[float]) -> float | None:
    return float(statistics.median(values)) if values else None


def make_schedule(stages: list[dict], repeats: int, seed: int) -> list[dict]:
    schedule: list[dict] = []
    for repeat in range(1, repeats + 1):
        order = [dict(stage) for stage in stages]
        random.Random(seed + repeat).shuffle(order)
        for stage in order:
            schedule.append({**stage, "repeat": repeat})
    return schedule


def resource_value(run: dict, key: str) -> float | None:
    value = (run.get("resources") or {}).get(key)
    return float(value) if isinstance(value, (int, float)) else None


def stage_summary(stage: dict, runs: list[dict], requested_repeats: int) -> dict:
    def vals(key: str) -> list[float]:
        return [float(r[key]) for r in runs if isinstance(r.get(key), (int, float))]

    realtime_cpu = [
        v
        for r in runs
        if (v := resource_value(r, "realtime_cpu_mean_pct")) is not None
    ]
    pg_cpu = [
        v
        for r in runs
        if (v := resource_value(r, "workload_pg_cpu_mean_pct")) is not None
    ]
    p95 = vals("p95_ms")

    return {
        **stage,
        "requested_repeats": requested_repeats,
        "n_valid_runs": len(runs),
        "p50_median_ms": median_or_none(vals("p50_ms")),
        "p95_median_ms": median_or_none(p95),
        "p95_min_ms": min(p95) if p95 else None,
        "p95_max_ms": max(p95) if p95 else None,
        "p50_commit_ack_median_ms": median_or_none(vals("p50_commit_ack_ms")),
        "p95_commit_ack_median_ms": median_or_none(vals("p95_commit_ack_ms")),
        "db_write_commit_p50_median_ms": median_or_none(vals("p50_db_write_commit_ms")),
        "delivery_median": median_or_none(vals("delivery_ratio")),
        "achieved_rate_median": median_or_none(vals("achieved_load_rate")),
        "loop_lag_median_ms": median_or_none(vals("max_loop_lag_ms")),
        "realtime_cpu_mean_median_pct": median_or_none(realtime_cpu),
        "workload_pg_cpu_mean_median_pct": median_or_none(pg_cpu),
        "all_requested_repeats_valid": len(runs) == requested_repeats,
        "all_delivered": bool(runs) and all(r.get("delivery_ratio") == 1.0 for r in runs),
        "runs": runs,
    }


def make_run_record(item, result, *, pos, attempt, apparatus_ok, bad):
    return {
        "position": pos,
        "repeat": int(item["repeat"]),
        "attempt": attempt,
        "stage": item["stage"],
        "queries": int(item["queries"]),
        "load_rate": float(item["load_rate"]),
        "latency_definition_primary": result.get("latency_definition_primary"),
        "latency_definition_secondary": result.get("latency_definition_secondary"),
        "p50_ms": result.get("p50_ms"),
        "p95_ms": result.get("p95_ms"),
        "p50_commit_ack_ms": result.get("p50_commit_ack_ms"),
        "p95_commit_ack_ms": result.get("p95_commit_ack_ms"),
        "p50_db_write_commit_ms": result.get("p50_db_write_commit_ms"),
        "delivery_ratio": result.get("delivery_ratio"),
        "missed": result.get("missed"),
        "wrong_query_index": result.get("wrong_query_index"),
        "measure_s": result.get("measure_s"),
        "measure_actual_s": result.get("measure_actual_s"),
        "probe_schedule_lag_max_ms": result.get("probe_schedule_lag_max_ms"),
        "achieved_load_rate": result.get("achieved_load_rate"),
        "load_achieved_ratio": result.get("load_achieved_ratio"),
        "max_loop_lag_ms": result.get("max_loop_lag_ms"),
        "latency_drift": result.get("latency_drift"),
        "resources": result.get("resources"),
        "valid": result.get("valid"),
        "apparatus_valid": apparatus_ok,
        "apparatus_invalid_reasons": bad,
        "raw_file": result.get("raw_file"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=("query", "write"), default="query")
    ap.add_argument(
        "--query-class",
        choices=("filter", "sorted", "window", "window_join", "window_search"),
        default="window",
    )
    ap.add_argument("--queries", required=True)
    ap.add_argument("--load-rate", type=float, default=None)
    ap.add_argument("--rates", default="")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--apparatus-retries", type=int, default=1)
    ap.add_argument("--clients", type=int, default=2)
    ap.add_argument("--registration-rate", type=float, default=200.0)
    ap.add_argument("--harness-cores", type=float, default=2.0)
    ap.add_argument("--needle", default="gamma")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--topics", type=int, default=30000)
    ap.add_argument("--users", type=int, default=1000)
    ap.add_argument("--expect-messages", type=int, default=1000000)
    ap.add_argument("--probes", type=int, default=100)
    ap.add_argument("--settle", type=float, default=30.0)
    ap.add_argument("--measure-seconds", type=float, default=60.0)
    ap.add_argument("--drain-seconds", type=float, default=30.0)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--ready-timeout", type=float, default=1200.0)
    ap.add_argument("--hit-ratio", type=float, default=1.0)
    ap.add_argument("--restart-wait", type=float, default=10.0)
    ap.add_argument("--shuffle-seed", type=int, default=42)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    if args.repeats <= 0 or args.apparatus_retries < 0:
        raise RuntimeError("ungueltige Repeat-/Retry-Zahl")
    if args.drain_seconds < args.timeout:
        raise RuntimeError("--drain-seconds muss >= --timeout sein")

    query_values = [int(x.strip()) for x in args.queries.split(",") if x.strip()]
    if not query_values or any(q <= 0 for q in query_values):
        raise RuntimeError("ungueltige --queries")

    if args.mode == "query":
        if args.load_rate is None or args.load_rate <= 0:
            raise RuntimeError("query-Modus braucht --load-rate > 0")
        stages = [
            {"stage": f"q{q}", "queries": q, "load_rate": float(args.load_rate)}
            for q in query_values
        ]
    else:
        if len(query_values) != 1:
            raise RuntimeError("write-Modus braucht genau eine feste Queryzahl")
        rates = [float(x.strip()) for x in args.rates.split(",") if x.strip()]
        if not rates or any(r <= 0 for r in rates):
            raise RuntimeError("write-Modus braucht positive --rates")
        stages = [
            {
                "stage": f"r{str(r).replace('.', 'p')}",
                "queries": query_values[0],
                "load_rate": r,
            }
            for r in rates
        ]

    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_dir = out_path.parent / (out_path.stem + "_runs")
    out_dir.mkdir(parents=True, exist_ok=True)

    schedule = make_schedule(stages, args.repeats, args.shuffle_seed)
    print("\nGeplante Messreihenfolge:")
    for pos, item in enumerate(schedule, start=1):
        print(
            f"  {pos:2d}. Repeat {item['repeat']}: "
            f"Q={item['queries']} Rate={item['load_rate']:g}/s"
        )

    valid_runs: list[dict] = []
    attempts: list[dict] = []
    failed_repeats: list[dict] = []

    for pos, item in enumerate(schedule, start=1):
        print("\n========================================")
        print(
            f"Lauf {pos}/{len(schedule)} | Repeat {item['repeat']} | "
            f"Q={item['queries']} | Rate={item['load_rate']:g}/s"
        )
        print("========================================")

        selected = None
        for attempt in range(1, args.apparatus_retries + 2):
            print(f"  Apparatur-Versuch {attempt}/{args.apparatus_retries + 1}")
            prepare_run(args)
            result = run_one(
                args,
                queries=int(item["queries"]),
                load_rate=float(item["load_rate"]),
                repeat=int(item["repeat"]),
                attempt=attempt,
                stage_label=str(item["stage"]),
                out_dir=out_dir,
            )
            apparatus_ok, bad = apparatus_valid(result)
            record = make_run_record(
                item,
                result,
                pos=pos,
                attempt=attempt,
                apparatus_ok=apparatus_ok,
                bad=bad,
            )
            attempts.append(record)

            if apparatus_ok:
                selected = record
                valid_runs.append(record)
                print(
                    f"  GUELTIG: p50={record['p50_ms']} ms, "
                    f"p95={record['p95_ms']} ms, "
                    f"Delivery={record['delivery_ratio']}, "
                    f"Rate={record['achieved_load_rate']}"
                )
                break
            print(f"  UNGUELTIGE APPARATUR: {bad}")

        if selected is None:
            failed_repeats.append(
                {
                    "position": pos,
                    "repeat": int(item["repeat"]),
                    "stage": item["stage"],
                    "queries": int(item["queries"]),
                    "load_rate": float(item["load_rate"]),
                    "reason": "all apparatus attempts invalid",
                }
            )

        out_path.write_text(
            json.dumps(
                {
                    "status": "running",
                    "system": "hasura",
                    "mode": args.mode,
                    "query_class": args.query_class,
                    "schedule": schedule,
                    "valid_runs": valid_runs,
                    "attempts": attempts,
                    "failed_repeats": failed_repeats,
                },
                indent=2,
            )
        )

    summaries = []
    for stage in stages:
        runs = [r for r in valid_runs if r["stage"] == stage["stage"]]
        summaries.append(stage_summary(stage, runs, args.repeats))

    status = "complete" if not failed_repeats else "complete_with_invalid_repeats"
    summary = {
        "status": status,
        "system": "hasura",
        "mode": args.mode,
        "query_class": args.query_class,
        "latency_definition_primary": "write_start_to_observation",
        "latency_definition_secondary": "commit_ack_to_observation",
        "capacity_interpretation": (
            "No automatic SLA/saturation cutoff. Identify the last stable stage "
            "before sustained queueing/latency growth, delivery degradation, or "
            "other system-overload evidence while apparatus remains valid."
        ),
        "hasura_live_query_refetch_ms": 1000,
        "hasura_live_query_batch_size": 100,
        "clients": args.clients,
        "registration_rate": args.registration_rate,
        "repeats": args.repeats,
        "apparatus_retries": args.apparatus_retries,
        "probes": args.probes,
        "settle_s": args.settle,
        "measure_s": args.measure_seconds,
        "drain_s": args.drain_seconds,
        "timeout_s": args.timeout,
        "restart_wait_s": args.restart_wait,
        "shuffle_seed": args.shuffle_seed,
        "dataset": {
            "messages": args.expect_messages,
            "topics": args.topics,
            "users": args.users,
        },
        "schedule": schedule,
        "stages": summaries,
        "valid_runs": valid_runs,
        "attempts": attempts,
        "failed_repeats": failed_repeats,
    }
    out_path.write_text(json.dumps(summary, indent=2))

    print("\n========================================")
    print("ZUSAMMENFASSUNG")
    print("========================================")
    print(
        f"{'Q':>7} {'Rate/s':>9} {'valid':>7} {'p50 med':>10} {'p95 med':>10} "
        f"{'Delivery':>10} {'HasuraCPU':>10} {'PG-CPU':>9}"
    )

    def fmt(v, width=10, dec=1):
        return f"{v:{width}.{dec}f}" if isinstance(v, (int, float)) else f"{'n/a':>{width}}"

    for stage in sorted(summaries, key=lambda x: (x["queries"], x["load_rate"])):
        print(
            f"{stage['queries']:7d} {stage['load_rate']:9.1f} "
            f"{stage['n_valid_runs']:7d} "
            f"{fmt(stage['p50_median_ms'])} {fmt(stage['p95_median_ms'])} "
            f"{fmt(stage['delivery_median'], 10, 3)} "
            f"{fmt(stage['realtime_cpu_mean_median_pct'], 10, 1)} "
            f"{fmt(stage['workload_pg_cpu_mean_median_pct'], 9, 1)}"
        )

    print(f"\nZusammenfassung gespeichert: {out_path}")
    if failed_repeats:
        print(
            "WARNUNG: Mindestens ein geplanter Repeat blieb trotz Retry apparativ "
            "ungueltig. Vor der finalen Auswertung gezielt nachmessen."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
