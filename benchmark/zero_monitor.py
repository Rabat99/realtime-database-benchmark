"""
Auslastungsmonitor fuer den Messlauf.

Zweck:
Nachweisen, dass nicht die Messapparatur der Engpass war und beobachten,
welches Ressourcenbudget des Zielsystems an seine Grenze geraet.

Erhoben werden:

* CPU-Anteil je Container ueber `docker stats`.
  100 % entspricht einem voll ausgelasteten CPU-Kern.

* Kombinierte CPU-Auslastung der Zero-spezifischen Komponenten
  zero-cache, postgres-meta und zero-query-api. Diese teilen sich in der
  Benchmarkumgebung dasselbe CPU-Budget.

* CPU-Auslastung der Benchmark-Clientcontainer als Kontrolle des Harness.

* Steal-Time aus /proc/stat. Sie zeigt CPU-Zeit, die der Hypervisor der VM
  entzieht. Geringfuegige Werte werden toleriert; die Gueltigkeitspruefung
  des Benchmarks verwirft Laeufe ab der definierten Grenze.

Monitorfehler werden im Ergebnis ausgegeben und muessen den Messlauf
ungueltig machen.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from pathlib import Path

import psycopg2


def load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out

    for raw in path.read_text().splitlines():
        raw = raw.strip()
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")

    return out


def read_cpu_times() -> tuple[int, int]:
    """
    Liefert (steal, total) aus der aggregierten CPU-Zeile von /proc/stat.
    """

    with open(
        "/proc/stat",
        "r",
        encoding="utf-8",
    ) as fh:
        for line in fh:
            if line.startswith("cpu "):
                parts = [
                    int(x)
                    for x in line.split()[1:]
                ]

                total = sum(parts)

                # Linux /proc/stat:
                # user nice system idle iowait irq softirq steal ...
                steal = (
                    parts[7]
                    if len(parts) > 7
                    else 0
                )

                return steal, total

    return 0, 0


class ResourceMonitor(threading.Thread):
    def __init__(
        self,
        *,
        repo: Path,
        compose_file: Path,
        env_file: Path,
        interval_s: float = 2.0,
    ) -> None:
        super().__init__(daemon=True)

        self.repo = repo
        self.compose_file = compose_file
        self.env_file = env_file
        self.interval_s = interval_s
        self.env = {**load_dotenv(env_file), **os.environ}

        # Nicht "_stop" nennen:
        # threading.Thread verwendet intern selbst _stop().
        self._stop_event = threading.Event()

        # Einzelwerte je Container.
        self.samples: dict[
            str,
            list[float],
        ] = {}

        # Gemeinsame Samples je Messzeitpunkt.
        # Damit koennen CPU-Gruppen korrekt summiert werden.
        self.sample_frames: list[
            dict[str, float]
        ] = []

        self.steal_pct: list[
            float
        ] = []

        self.replication_lag_series: list[
            dict[str, object]
        ] = []

        self.errors: list[
            str
        ] = []

    def stop(self) -> None:
        self._stop_event.set()

    def _sample_docker(
        self,
    ) -> None:
        """
        Holt einen gemeinsamen CPU-Snapshot aller laufenden Container.
        """

        res = subprocess.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.Name}};{{.CPUPerc}}",
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )

        if res.returncode != 0:
            self.errors.append(
                res.stderr.strip()[:200]
            )
            return

        frame: dict[
            str,
            float
        ] = {}

        for line in (
            res.stdout
            .strip()
            .splitlines()
        ):
            if ";" not in line:
                continue

            name, cpu = line.split(
                ";",
                1,
            )

            name = name.strip()

            try:
                value = float(
                    cpu.strip().rstrip("%")
                )

            except ValueError:
                continue

            frame[name] = value

            self.samples.setdefault(
                name,
                [],
            ).append(
                value
            )

        if frame:
            self.sample_frames.append(
                frame
            )

    def _sample_replication_lag(self, conn) -> None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT slot_name,
                           active,
                           coalesce(
                             pg_wal_lsn_diff(
                               pg_current_wal_lsn(), confirmed_flush_lsn
                             ), 0
                           )::bigint AS lag_bytes
                    FROM pg_replication_slots
                    WHERE slot_type = 'logical'
                    ORDER BY slot_name
                    """
                )

                slots = [
                    {
                        "slot_name": str(row[0]),
                        "active": bool(row[1]),
                        "lag_bytes": int(row[2]),
                    }
                    for row in cur.fetchall()
                ]

            self.replication_lag_series.append(
                {
                    "t_monotonic": time.monotonic(),
                    "max_lag_bytes": max(
                        (int(slot["lag_bytes"]) for slot in slots),
                        default=0,
                    ),
                    "slots": slots,
                }
            )

        except Exception as exc:  # noqa: BLE001
            self.errors.append(
                f"replication lag: {str(exc)[:160]}"
            )

    def run(self) -> None:
        prev_steal, prev_total = (
            read_cpu_times()
        )

        repl_conn = None

        try:
            repl_conn = psycopg2.connect(
                host="127.0.0.1",
                port=int(
                    self.env.get(
                        "POSTGRES_PORT",
                        "5432",
                    )
                ),
                user=self.env.get(
                    "POSTGRES_USER",
                    "postgres",
                ),
                password=self.env.get(
                    "POSTGRES_PASSWORD",
                    "postgres",
                ),
                dbname=self.env.get(
                    "POSTGRES_DB",
                    "postgres",
                ),
            )
            repl_conn.autocommit = True

        except Exception as exc:  # noqa: BLE001
            self.errors.append(
                f"replication monitor connect: {str(exc)[:160]}"
            )

        try:
            while not self._stop_event.is_set():
                # Event.wait() statt time.sleep():
                # stop() kann den Thread dadurch sofort aufwecken.
                if self._stop_event.wait(
                    self.interval_s
                ):
                    break

                steal, total = (
                    read_cpu_times()
                )

                d_total = (
                    total
                    - prev_total
                )

                d_steal = (
                    steal
                    - prev_steal
                )

                if d_total > 0:
                    self.steal_pct.append(
                        100.0
                        * d_steal
                        / d_total
                    )

                prev_steal = steal
                prev_total = total

                try:
                    self._sample_docker()

                except Exception as exc:  # noqa: BLE001
                    self.errors.append(
                        str(exc)[:200]
                    )

                if repl_conn is not None:
                    self._sample_replication_lag(
                        repl_conn
                    )

        finally:
            if repl_conn is not None:
                try:
                    repl_conn.close()

                except Exception:  # noqa: BLE001
                    pass

    @staticmethod
    def _matches_any_prefix(
        name: str,
        prefixes: tuple[
            str,
            ...,
        ],
    ) -> bool:
        return name.startswith(
            prefixes
        )

    def _group_samples(
        self,
        prefixes: tuple[
            str,
            ...,
        ],
    ) -> list[float]:
        """
        Summiert fuer jeden gemeinsamen docker-stats-Snapshot
        die CPU-Werte aller Container einer Gruppe.

        Dadurch werden nicht unabhaengige Sample-Listen anhand
        ihrer Listenposition miteinander verrechnet.
        """

        values: list[
            float
        ] = []

        for frame in self.sample_frames:
            total = sum(
                cpu
                for name, cpu
                in frame.items()
                if self._matches_any_prefix(
                    name,
                    prefixes,
                )
            )

            # Nur einen Sample aufnehmen,
            # wenn mindestens ein Container
            # dieser Gruppe vorhanden war.
            found = any(
                self._matches_any_prefix(
                    name,
                    prefixes,
                )
                for name
                in frame
            )

            if found:
                values.append(
                    total
                )

        return values

    def report(
        self,
        harness_prefixes: tuple[
            str,
            ...,
        ] = (
            "zero-bench-",
        ),
        zero_path_prefixes: tuple[
            str,
            ...,
        ] = (
            "compose-zero-cache-",
            "compose-postgres-meta-",
            "compose-zero-query-api-",
        ),
        workload_pg_prefixes: tuple[
            str,
            ...,
        ] = (
            "compose-postgres-",
        ),
    ) -> dict:
        """
        Erstellt die Zusammenfassung fuer das Ergebnis-JSON.

        harness_cpu_*:
            CPU-Nutzung der Zero-Clientcontainer auf den
            Harness-Kernen.

        zero_path_cpu_*:
            Summierte CPU-Nutzung der Zero-spezifischen
            Serverkomponenten, die dasselbe CPU-Budget teilen.

        workload_pg_cpu_*:
            CPU-Nutzung des gemeinsamen Workload-PostgreSQL.
        """

        per_container = {}

        for (
            name,
            values,
        ) in self.samples.items():
            if not values:
                continue

            per_container[name] = {
                "mean_pct": round(
                    sum(values)
                    / len(values),
                    1,
                ),
                "max_pct": round(
                    max(values),
                    1,
                ),
                "samples": len(
                    values
                ),
            }

        harness_samples = (
            self._group_samples(
                harness_prefixes
            )
        )

        zero_path_samples = (
            self._group_samples(
                zero_path_prefixes
            )
        )

        # Das Praefix "compose-postgres-" umfasst auch postgres-meta.
        # Der Workload-PostgreSQL wird deshalb exakt gefiltert.
        workload_pg_samples: list[
            float
        ] = []

        for frame in self.sample_frames:
            values = [
                cpu
                for name, cpu
                in frame.items()
                if (
                    name.startswith(
                        workload_pg_prefixes
                    )
                    and not name.startswith(
                        "compose-postgres-meta-"
                    )
                )
            ]

            if values:
                workload_pg_samples.append(
                    sum(values)
                )

        harness_container_values = [
            data["max_pct"]
            for name, data
            in per_container.items()
            if self._matches_any_prefix(
                name,
                harness_prefixes,
            )
        ]

        lag_values = [
            int(sample["max_lag_bytes"])
            for sample in self.replication_lag_series
            if isinstance(
                sample.get("max_lag_bytes"),
                int,
            )
        ]

        return {
            "per_container": (
                per_container
            ),

            # --------------------------------------------------
            # Harness
            # --------------------------------------------------

            "harness_cpu_mean_pct": (
                round(
                    sum(harness_samples)
                    / len(harness_samples),
                    1,
                )
                if harness_samples
                else None
            ),

            "harness_cpu_max_pct": (
                round(
                    max(harness_samples),
                    1,
                )
                if harness_samples
                else None
            ),

            "harness_container_max_pct": (
                round(
                    max(
                        harness_container_values
                    ),
                    1,
                )
                if harness_container_values
                else None
            ),

            # --------------------------------------------------
            # Zero-spezifischer Serverpfad
            # Gemeinsames CPU-Budget des Zero-Serverpfads.
            # --------------------------------------------------

            "zero_path_cpu_mean_pct": (
                round(
                    sum(
                        zero_path_samples
                    )
                    / len(
                        zero_path_samples
                    ),
                    1,
                )
                if zero_path_samples
                else None
            ),

            "zero_path_cpu_max_pct": (
                round(
                    max(
                        zero_path_samples
                    ),
                    1,
                )
                if zero_path_samples
                else None
            ),

            # --------------------------------------------------
            # Gemeinsames Workload-PostgreSQL
            # --------------------------------------------------

            "workload_pg_cpu_mean_pct": (
                round(
                    sum(
                        workload_pg_samples
                    )
                    / len(
                        workload_pg_samples
                    ),
                    1,
                )
                if workload_pg_samples
                else None
            ),

            "workload_pg_cpu_max_pct": (
                round(
                    max(
                        workload_pg_samples
                    ),
                    1,
                )
                if workload_pg_samples
                else None
            ),

            # --------------------------------------------------
            # Hypervisor
            # --------------------------------------------------

            "steal_mean_pct": (
                round(
                    sum(self.steal_pct)
                    / len(self.steal_pct),
                    3,
                )
                if self.steal_pct
                else None
            ),

            "steal_max_pct": (
                round(
                    max(self.steal_pct),
                    3,
                )
                if self.steal_pct
                else None
            ),

            # --------------------------------------------------
            # Rueckstand der logischen PostgreSQL-Replikation
            # --------------------------------------------------

            "replication_lag_start_bytes": (
                lag_values[0]
                if lag_values
                else None
            ),

            "replication_lag_end_bytes": (
                lag_values[-1]
                if lag_values
                else None
            ),

            "replication_lag_max_bytes": (
                max(lag_values)
                if lag_values
                else None
            ),

            "replication_lag_growth_bytes": (
                lag_values[-1] - lag_values[0]
                if len(lag_values) >= 2
                else None
            ),

            "replication_lag_series": (
                self.replication_lag_series
            ),

            # --------------------------------------------------
            # Monitor selbst
            # --------------------------------------------------

            "monitor_samples": len(
                self.sample_frames
            ),

            "monitor_errors": (
                self.errors[:3]
            ),
        }
