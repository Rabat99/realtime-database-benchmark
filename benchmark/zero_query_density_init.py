#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import json
import statistics
from pathlib import Path

from zero_adapter import ZeroSubscriberPool


def percentile(values, p):
    if not values:
        return None

    xs = sorted(values)

    if len(xs) == 1:
        return xs[0]

    pos = (len(xs) - 1) * (p / 100.0)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    fraction = pos - lo

    return (
        xs[lo] * (1.0 - fraction)
        + xs[hi] * fraction
    )


def summarize(values):
    if not values:
        return None

    return {
        "p50": percentile(values, 50),
        "p90": percentile(values, 90),
        "p95": percentile(values, 95),
        "min": min(values),
        "max": max(values),
        "mean": statistics.mean(values),
    }


def concurrency_profile(intervals):
    """
    intervals:
        [(query_start_ns, query_ready_ns), ...]

    Ermittelt die tatsaechliche Ueberlappung der laufenden
    Query-Hydrationen.

    In diesem Query-Density-Test ist eine vollstaendige N-fache
    Ueberlappung KEINE Validitaetsbedingung. Bei vielen Queries pro
    Client werden die Materialisierungen innerhalb eines Clients
    nacheinander registriert, waehrend fruehe Queries bereits fertig
    werden koennen.
    """

    if not intervals:
        return None

    events = []

    for start_ns, ready_ns in intervals:
        events.append((start_ns, 1))
        events.append((ready_ns, -1))

    # Bei identischem Zeitstempel wird ein Ende vor einem neuen
    # Start verarbeitet.
    events.sort(
        key=lambda item: (
            item[0],
            item[1],
        )
    )

    first_ns = min(
        start_ns
        for start_ns, _ in intervals
    )

    last_ns = max(
        ready_ns
        for _, ready_ns in intervals
    )

    span_ns = last_ns - first_ns

    current = 0
    peak = 0
    weighted_ns = 0.0
    previous_ns = events[0][0]

    for timestamp_ns, delta in events:
        if timestamp_ns > previous_ns:
            weighted_ns += (
                current
                * (timestamp_ns - previous_ns)
            )

        current += delta
        peak = max(
            peak,
            current,
        )
        previous_ns = timestamp_ns

    busy_ns = sum(
        ready_ns - start_ns
        for start_ns, ready_ns in intervals
    )

    return {
        "peak_concurrent_hydrations":
            peak,

        "time_weighted_mean_concurrency":
            (
                weighted_ns / span_ns
                if span_ns > 0
                else None
            ),

        "little_mean_concurrency":
            (
                busy_ns / span_ns
                if span_ns > 0
                else None
            ),

        "wall_span_ms":
            (
                span_ns / 1_000_000.0
                if span_ns >= 0
                else None
            ),
    }


class QueryDensityPool(ZeroSubscriberPool):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.query_start_ns = {}
        self.query_ready_ns = {}

        self.client_connected_ns = {}
        self.client_armed_ns = {}
        self.client_released_ns = {}

        self.event_types = {}

    def _dispatch(self, ev):
        typ = ev.get("type")

        client_id = str(
            ev.get(
                "client_id",
                "",
            )
        )

        timestamp_raw = ev.get(
            "t_epoch_ns"
        )

        timestamp_ns = (
            int(timestamp_raw)
            if timestamp_raw is not None
            else None
        )

        if typ:
            slot = self.event_types.setdefault(
                typ,
                {
                    "count": 0,
                    "first_ns": None,
                    "last_ns": None,
                },
            )

            slot["count"] += 1

            if timestamp_ns is not None:
                if (
                    slot["first_ns"] is None
                    or timestamp_ns < slot["first_ns"]
                ):
                    slot["first_ns"] = timestamp_ns

                if (
                    slot["last_ns"] is None
                    or timestamp_ns > slot["last_ns"]
                ):
                    slot["last_ns"] = timestamp_ns

        if (
            typ == "connected"
            and timestamp_ns is not None
        ):
            self.client_connected_ns[
                client_id
            ] = timestamp_ns

        elif (
            typ == "armed"
            and timestamp_ns is not None
        ):
            self.client_armed_ns[
                client_id
            ] = timestamp_ns

        elif (
            typ == "released"
            and timestamp_ns is not None
        ):
            self.client_released_ns[
                client_id
            ] = timestamp_ns

        elif (
            typ == "query-start"
            and timestamp_ns is not None
        ):
            query_index = int(
                ev["query_index"]
            )

            self.query_start_ns[
                (
                    client_id,
                    query_index,
                )
            ] = timestamp_ns

        elif (
            typ == "query-ready"
            and timestamp_ns is not None
        ):
            query_index = int(
                ev["query_index"]
            )

            self.query_ready_ns[
                (
                    client_id,
                    query_index,
                )
            ] = timestamp_ns

        super()._dispatch(ev)


async def run(args):
    repo = (
        Path(__file__)
        .resolve()
        .parents[1]
    )

    pool = QueryDensityPool(
        repo_root=repo,
        total_queries=args.subscribers,
        clients=args.clients,
        query_class=args.query_class,
        topics=args.topics,
        limit=args.limit,
        needle=args.needle,
    )

    barrier_release_ns = None

    try:
        # --------------------------------------------------------------
        # Barrier-Modus vor dem Start aktivieren.
        # --------------------------------------------------------------

        await pool.arm()

        await pool.start()

        # --------------------------------------------------------------
        # Alle Zero-Clients muessen bereits verbunden und an der
        # Barriere angekommen sein, bevor Queries materialisiert werden.
        # --------------------------------------------------------------

        try:
            await pool.wait_connected(
                timeout=args.connect_timeout
            )

            await pool.wait_armed(
                timeout=args.connect_timeout
            )

        except Exception:
            print()
            print(
                "===== ZERO QUERY-DENSITY CONNECTION FAILURE ====="
            )

            print(
                "connected_clients:",
                len(pool._connected_clients),
            )

            print(
                "armed_clients:",
                len(pool._armed_clients),
            )

            print(
                "ready_clients:",
                len(pool._ready_clients),
            )

            print(
                "errors:",
                pool.errors,
            )

            print(
                "query_started:",
                len(pool.query_start_ns),
            )

            print(
                "query_ready:",
                len(pool.query_ready_ns),
            )

            print()
            print(
                "===== LAST CLIENT LOGS ====="
            )

            for line in list(pool.logs)[-100:]:
                print(line)

            print(
                "============================="
            )
            print()

            raise

        # Bereits geschriebene stdout-Events noch verarbeiten lassen.
        await asyncio.sleep(0.25)

        # --------------------------------------------------------------
        # Vor dem gemeinsamen GO darf keine gemessene Query laufen.
        # --------------------------------------------------------------

        if pool.query_start_ns:
            raise RuntimeError(
                "Barrier invalid: "
                f"{len(pool.query_start_ns)} "
                "queries started before release()."
            )

        # --------------------------------------------------------------
        # Gemeinsames GO.
        # --------------------------------------------------------------

        barrier_release_ns = (
            await pool.release()
        )

        # --------------------------------------------------------------
        # wait_ready() wartet beim bestehenden Zero-Adapter darauf,
        # dass jeder Zero-Client sein vollstaendiges Query-Set als
        # initialisiert gemeldet hat.
        # --------------------------------------------------------------

        try:
            await pool.wait_ready(
                timeout=args.timeout
            )

        except Exception:
            print()
            print(
                "===== ZERO QUERY-DENSITY FAILURE ====="
            )

            print(
                "connected_clients:",
                len(pool._connected_clients),
            )

            print(
                "armed_clients:",
                len(pool._armed_clients),
            )

            print(
                "released_clients:",
                len(pool._released_clients),
            )

            print(
                "ready_clients:",
                len(pool._ready_clients),
            )

            print(
                "errors:",
                pool.errors,
            )

            print(
                "query_started:",
                len(pool.query_start_ns),
            )

            print(
                "query_ready:",
                len(pool.query_ready_ns),
            )

            print()
            print(
                "===== LAST CLIENT LOGS ====="
            )

            for line in list(pool.logs)[-100:]:
                print(line)

            print(
                "============================="
            )
            print()

            raise

        # --------------------------------------------------------------
        # Nur Queries mit Start- UND Ready-Event auswerten.
        # --------------------------------------------------------------

        common_keys = (
            set(pool.query_start_ns)
            &
            set(pool.query_ready_ns)
        )

        keys = sorted(
            common_keys,
            key=lambda key:
                pool.query_start_ns[key],
        )

        samples = []
        intervals = []

        for key in keys:
            client_id, query_index = key

            start_ns = (
                pool.query_start_ns[key]
            )

            ready_ns = (
                pool.query_ready_ns[key]
            )

            if ready_ns < start_ns:
                continue

            intervals.append(
                (
                    start_ns,
                    ready_ns,
                )
            )

            client_release_ns = (
                pool.client_released_ns.get(
                    client_id
                )
            )

            client_release_to_start_ms = (
                (
                    start_ns
                    - client_release_ns
                )
                / 1_000_000.0
                if client_release_ns
                is not None
                else None
            )

            samples.append(
                {
                    "client_id":
                        client_id,

                    "query_index":
                        query_index,

                    "start_epoch_ns":
                        start_ns,

                    "ready_epoch_ns":
                        ready_ns,

                    "start_offset_from_python_release_ms":
                        (
                            (
                                start_ns
                                - barrier_release_ns
                            )
                            / 1_000_000.0
                        ),

                    "client_release_to_start_ms":
                        client_release_to_start_ms,

                    "ready_ms":
                        (
                            (
                                ready_ns
                                - start_ns
                            )
                            / 1_000_000.0
                        ),
                }
            )

        ready_values = [
            sample["ready_ms"]
            for sample in samples
        ]

        start_times_ns = [
            sample["start_epoch_ns"]
            for sample in samples
        ]

        python_release_offsets = [
            sample[
                "start_offset_from_python_release_ms"
            ]
            for sample in samples
        ]

        client_release_offsets = [
            sample[
                "client_release_to_start_ms"
            ]
            for sample in samples
            if (
                sample[
                    "client_release_to_start_ms"
                ]
                is not None
            )
        ]

        # --------------------------------------------------------------
        # Startstreuung ist in diesem Test nur ein Diagnosewert.
        # Bei vielen Queries pro Client ist eine wachsende Streuung
        # erwartbar und darf nicht allein den Lauf invalidieren.
        # --------------------------------------------------------------

        if start_times_ns:
            start_spread_ms = (
                (
                    max(start_times_ns)
                    - min(start_times_ns)
                )
                / 1_000_000.0
            )
        else:
            start_spread_ms = None

        ready_summary = summarize(
            ready_values
        )

        if (
            ready_summary is not None
            and ready_summary["p50"] > 0
            and start_spread_ms is not None
        ):
            start_spread_ratio = (
                start_spread_ms
                / ready_summary["p50"]
            )
        else:
            start_spread_ratio = None

        if start_spread_ratio is not None:
            start_spread_reference_ok = (
                start_spread_ratio
                <= args.max_start_spread_ratio
            )
        else:
            start_spread_reference_ok = None

        # --------------------------------------------------------------
        # Gesamtzeitpunkte des Query-Density-Laufs.
        # --------------------------------------------------------------

        if samples:
            first_start_ns = min(
                sample["start_epoch_ns"]
                for sample in samples
            )

            last_start_ns = max(
                sample["start_epoch_ns"]
                for sample in samples
            )

            first_ready_ns = min(
                sample["ready_epoch_ns"]
                for sample in samples
            )

            last_ready_ns = max(
                sample["ready_epoch_ns"]
                for sample in samples
            )

            release_to_first_start_ms = (
                (
                    first_start_ns
                    - barrier_release_ns
                )
                / 1_000_000.0
            )

            release_to_last_start_ms = (
                (
                    last_start_ns
                    - barrier_release_ns
                )
                / 1_000_000.0
            )

            registration_span_ms = (
                (
                    last_start_ns
                    - first_start_ns
                )
                / 1_000_000.0
            )

            release_to_first_ready_ms = (
                (
                    first_ready_ns
                    - barrier_release_ns
                )
                / 1_000_000.0
            )

            release_to_last_ready_ms = (
                (
                    last_ready_ns
                    - barrier_release_ns
                )
                / 1_000_000.0
            )

            ready_span_ms = (
                (
                    last_ready_ns
                    - first_ready_ns
                )
                / 1_000_000.0
            )

            completion_rate_per_s = (
                args.subscribers
                / (
                    (
                        last_ready_ns
                        - first_start_ns
                    )
                    / 1_000_000_000.0
                )
                if last_ready_ns > first_start_ns
                else None
            )

            registration_rate_per_s = (
                args.subscribers
                / (
                    (
                        last_start_ns
                        - first_start_ns
                    )
                    / 1_000_000_000.0
                )
                if last_start_ns > first_start_ns
                else None
            )

        else:
            release_to_first_start_ms = None
            release_to_last_start_ms = None
            registration_span_ms = None
            release_to_first_ready_ms = None
            release_to_last_ready_ms = None
            ready_span_ms = None
            completion_rate_per_s = None
            registration_rate_per_s = None

        concurrency = concurrency_profile(
            intervals
        )

        query_started_count = len(
            pool.query_start_ns
        )

        query_ready_event_count = len(
            pool.query_ready_ns
        )

        measured_count = len(
            ready_values
        )

        max_loop_lag_ms = (
            pool.max_loop_lag_ms
        )

        # --------------------------------------------------------------
        # Harte Validitaetsbedingungen.
        #
        # full_concurrency_observed ist hier nur ein Diagnosewert, da die
        # Query-Dichte pro Client und nicht die simultane Freigabe gemessen wird.
        # --------------------------------------------------------------

        all_clients_connected = (
            len(pool._connected_clients)
            == args.clients
        )

        all_clients_armed = (
            len(pool._armed_clients)
            == args.clients
        )

        all_clients_released = (
            len(pool._released_clients)
            == args.clients
        )

        all_queries_started = (
            query_started_count
            == args.subscribers
        )

        all_queries_ready = (
            query_ready_event_count
            == args.subscribers
            and measured_count
            == args.subscribers
        )

        no_client_errors = (
            len(pool.errors) == 0
        )

        no_query_before_client_release = True

        for sample in samples:
            client_id = (
                sample["client_id"]
            )

            release_ns = (
                pool.client_released_ns.get(
                    client_id
                )
            )

            if (
                release_ns is None
                or
                sample["start_epoch_ns"]
                < release_ns
            ):
                no_query_before_client_release = False
                break

        harness_loop_lag_ok = (
            max_loop_lag_ms
            <= args.max_loop_lag_ms
        )

        valid = {
            "all_clients_connected":
                all_clients_connected,

            "all_clients_armed":
                all_clients_armed,

            "all_clients_released":
                all_clients_released,

            "all_queries_started":
                all_queries_started,

            "all_queries_ready":
                all_queries_ready,

            "no_client_errors":
                no_client_errors,

            "no_query_before_client_release":
                no_query_before_client_release,

            "harness_loop_lag_ok":
                harness_loop_lag_ok,
        }

        valid_run = all(
            valid.values()
        )

        # --------------------------------------------------------------
        # Ergebnis.
        # --------------------------------------------------------------

        result = {
            "system":
                "zero",

            "test":
                "subscription_query_density_init",

            "purpose":
                (
                    "Query-density scaling with a fixed number of "
                    "Zero client processes and an increasing number "
                    "of distinct materialized queries. This does not "
                    "imply Hasura-style batching or multiplexing."
                ),

            "subscribers":
                args.subscribers,

            "clients":
                args.clients,

            "queries_per_client":
                (
                    args.subscribers
                    / args.clients
                ),

            "query_class":
                args.query_class,

            "limit":
                args.limit,

            "topics":
                args.topics,

            "barrier":
                True,

            "measurement_definition":
                (
                    "query_materialize_start"
                    "_to_first_complete"
                ),

            "batch_measurement_definition":
                (
                    "common_barrier_release"
                    "_to_last_initial_result"
                ),

            "connection_definition":
                (
                    "Zero connection state "
                    "connected before barrier arm"
                ),

            "connected_client_count":
                len(
                    pool._connected_clients
                ),

            "armed_client_count":
                len(
                    pool._armed_clients
                ),

            "released_client_count":
                len(
                    pool._released_clients
                ),

            "query_started_count":
                query_started_count,

            "query_ready_event_count":
                query_ready_event_count,

            "ready_count":
                measured_count,

            "missing_ready_measurements":
                (
                    args.subscribers
                    - measured_count
                ),

            "ready_ms":
                ready_summary,

            "start_spread_ms":
                start_spread_ms,

            "start_spread_ratio_to_p50":
                start_spread_ratio,

            "start_spread_diagnostic": {
                "reference_ratio":
                    args.max_start_spread_ratio,

                "within_reference":
                    start_spread_reference_ok,

                "validity_gate":
                    False,

                "note":
                    (
                        "Expected to increase when many queries are "
                        "materialized over a fixed set of Zero clients."
                    ),
            },

            "client_release_to_start_ms":
                summarize(
                    client_release_offsets
                ),

            "python_release_to_start_ms":
                summarize(
                    python_release_offsets
                ),

            "batch": {
                "release_to_first_start_ms":
                    release_to_first_start_ms,

                "release_to_last_start_ms":
                    release_to_last_start_ms,

                "registration_span_ms":
                    registration_span_ms,

                "registration_rate_per_s":
                    registration_rate_per_s,

                "release_to_first_ready_ms":
                    release_to_first_ready_ms,

                "release_to_last_ready_ms":
                    release_to_last_ready_ms,

                "ready_span_ms":
                    ready_span_ms,

                "completion_rate_per_s":
                    completion_rate_per_s,
            },

            "concurrency":
                concurrency,

            "concurrency_diagnostic": {
                "validity_gate":
                    False,

                "note":
                    (
                        "Full N-way overlap is intentionally not "
                        "required in this fixed-client query-density "
                        "experiment."
                    ),
            },

            "harness": {
                "max_loop_lag_ms":
                    max_loop_lag_ms,

                "max_allowed_loop_lag_ms":
                    args.max_loop_lag_ms,

                "loop_lag_ms_per_client":
                    dict(
                        pool.loop_lag_ms
                    ),
            },

            "observed_event_types":
                pool.event_types,

            "valid":
                valid,

            "valid_run":
                valid_run,

            "samples":
                samples,

            "errors":
                pool.errors,
        }

        terminal_result = {
            key: value
            for key, value in result.items()
            if key != "samples"
        }

        print(
            json.dumps(
                terminal_result,
                indent=2,
            )
        )

        if args.json_out:
            output_path = Path(
                args.json_out
            )

            output_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            output_path.write_text(
                json.dumps(
                    result,
                    indent=2,
                ),
                encoding="utf-8",
            )

        if not valid_run:
            raise RuntimeError(
                "Query-density init run is methodologically invalid. "
                "Check the valid fields above."
            )

    finally:
        await pool.close()


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Zero query-density subscription-init benchmark "
            "with fixed client count and synchronized barrier."
        )
    )

    parser.add_argument(
        "--subscribers",
        type=int,
        default=1000,
    )

    parser.add_argument(
        "--clients",
        type=int,
        default=100,
    )

    parser.add_argument(
        "--query-class",
        choices=("filter", "sorted", "window", "window_join", "window_search"),
        default="window",
    )

    parser.add_argument(
        "--topics",
        type=int,
        default=30000,
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=10,
    )

    parser.add_argument(
        "--needle",
        default="gamma",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=900.0,
    )

    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=600.0,
    )

    parser.add_argument(
        "--max-loop-lag-ms",
        type=float,
        default=300.0,
    )

    parser.add_argument(
        "--max-start-spread-ratio",
        type=float,
        default=0.20,
    )

    parser.add_argument(
        "--barrier",
        action="store_true",
    )

    parser.add_argument(
        "--json-out",
        default="",
    )

    args = parser.parse_args()

    if args.subscribers <= 0:
        raise SystemExit(
            "--subscribers must be > 0."
        )

    if args.clients <= 0:
        raise SystemExit(
            "--clients must be > 0."
        )

    if args.clients > args.subscribers:
        raise SystemExit(
            "--clients must not be greater "
            "than --subscribers."
        )

    if args.subscribers > args.topics:
        raise SystemExit(
            "--subscribers must not exceed --topics "
            "because this experiment assigns one distinct "
            "query index across the configured topic range."
        )

    if not args.barrier:
        raise SystemExit(
            "Query-density init measurements require --barrier."
        )

    asyncio.run(
        run(args)
    )


if __name__ == "__main__":
    main()
