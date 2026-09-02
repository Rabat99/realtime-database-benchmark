"""
Gemeinsamer Ressourcenmonitor fuer Hasura- und Zero-Messlaeufe.

Erfasst:
* CPU je Docker-Container (docker stats, 100 % = ein CPU-Kern),
* CPU des Python-Harness-Prozesses,
* gruppierte CPU des Realtime-Pfads,
* CPU des gemeinsamen Workload-PostgreSQL,
* Steal-Time der VM,
* optional den Rueckstand logischer PostgreSQL-Replikationsslots.

Die gruppierten Werte werden aus demselben docker-stats-Snapshot gebildet.
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
        k, v = raw.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def read_cpu_times() -> tuple[int, int]:
    with open("/proc/stat", "r", encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("cpu "):
                parts = [int(x) for x in line.split()[1:]]
                total = sum(parts)
                steal = parts[7] if len(parts) > 7 else 0
                return steal, total
    return 0, 0


def read_process_cpu_ticks() -> int:
    """utime + stime des aktuellen Python-Prozesses in Clock-Ticks."""
    with open("/proc/self/stat", "r", encoding="utf-8") as fh:
        parts = fh.read().split()
    return int(parts[13]) + int(parts[14])


class ResourceMonitor(threading.Thread):
    def __init__(
        self,
        *,
        repo: Path,
        interval_s: float = 2.0,
        realtime_prefixes: tuple[str, ...],
        harness_container_prefixes: tuple[str, ...] = (),
        workload_pg_prefixes: tuple[str, ...] = ("compose-postgres-",),
        exclude_pg_prefixes: tuple[str, ...] = ("compose-postgres-meta-",),
        track_replication_lag: bool = False,
    ) -> None:
        super().__init__(daemon=True)
        self.repo = repo
        self.interval_s = interval_s
        self.realtime_prefixes = realtime_prefixes
        self.harness_container_prefixes = harness_container_prefixes
        self.workload_pg_prefixes = workload_pg_prefixes
        self.exclude_pg_prefixes = exclude_pg_prefixes
        self.track_replication_lag = track_replication_lag

        self.env = {**load_dotenv(repo / ".env"), **os.environ}
        self._stop_event = threading.Event()

        self.samples: dict[str, list[float]] = {}
        self.sample_frames: list[dict[str, float]] = []
        self.steal_pct: list[float] = []
        self.process_cpu_pct: list[float] = []
        self.replication_lag_series: list[dict[str, object]] = []
        self.errors: list[str] = []

    def stop(self) -> None:
        self._stop_event.set()

    @staticmethod
    def _matches_any_prefix(name: str, prefixes: tuple[str, ...]) -> bool:
        return bool(prefixes) and name.startswith(prefixes)

    def _sample_docker(self) -> None:
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
            self.errors.append(res.stderr.strip()[:200])
            return

        frame: dict[str, float] = {}
        for line in res.stdout.strip().splitlines():
            if ";" not in line:
                continue
            name, cpu = line.split(";", 1)
            name = name.strip()
            try:
                value = float(cpu.strip().rstrip("%"))
            except ValueError:
                continue
            frame[name] = value
            self.samples.setdefault(name, []).append(value)
        if frame:
            self.sample_frames.append(frame)

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
                        "slot_name": str(r[0]),
                        "active": bool(r[1]),
                        "lag_bytes": int(r[2]),
                    }
                    for r in cur.fetchall()
                ]
            self.replication_lag_series.append(
                {
                    "t_monotonic": time.monotonic(),
                    "max_lag_bytes": max(
                        (int(x["lag_bytes"]) for x in slots), default=0
                    ),
                    "slots": slots,
                }
            )
        except Exception as exc:  # noqa: BLE001
            self.errors.append(f"replication lag: {str(exc)[:160]}")

    def run(self) -> None:
        prev_steal, prev_total = read_cpu_times()
        prev_proc = read_process_cpu_ticks()
        prev_wall = time.monotonic()
        clk_tck = float(os.sysconf(os.sysconf_names["SC_CLK_TCK"]))

        repl_conn = None
        if self.track_replication_lag:
            try:
                repl_conn = psycopg2.connect(
                    host="127.0.0.1",
                    port=int(self.env.get("POSTGRES_PORT", "5432")),
                    user=self.env.get("POSTGRES_USER", "postgres"),
                    password=self.env.get("POSTGRES_PASSWORD", "postgres"),
                    dbname=self.env.get("POSTGRES_DB", "postgres"),
                )
                repl_conn.autocommit = True
            except Exception as exc:  # noqa: BLE001
                self.errors.append(f"replication monitor connect: {str(exc)[:160]}")

        try:
            while not self._stop_event.is_set():
                if self._stop_event.wait(self.interval_s):
                    break

                now_wall = time.monotonic()
                steal, total = read_cpu_times()
                proc = read_process_cpu_ticks()

                d_total = total - prev_total
                d_steal = steal - prev_steal
                if d_total > 0:
                    self.steal_pct.append(100.0 * d_steal / d_total)

                d_wall = now_wall - prev_wall
                d_proc_s = (proc - prev_proc) / clk_tck
                if d_wall > 0:
                    self.process_cpu_pct.append(100.0 * d_proc_s / d_wall)

                prev_steal, prev_total = steal, total
                prev_proc, prev_wall = proc, now_wall

                try:
                    self._sample_docker()
                except Exception as exc:  # noqa: BLE001
                    self.errors.append(str(exc)[:200])

                if repl_conn is not None:
                    self._sample_replication_lag(repl_conn)
        finally:
            if repl_conn is not None:
                try:
                    repl_conn.close()
                except Exception:  # noqa: BLE001
                    pass

    def _group_samples(self, prefixes: tuple[str, ...]) -> list[float]:
        values: list[float] = []
        if not prefixes:
            return values
        for frame in self.sample_frames:
            selected = [
                cpu
                for name, cpu in frame.items()
                if self._matches_any_prefix(name, prefixes)
            ]
            if selected:
                values.append(sum(selected))
        return values

    def _workload_pg_samples(self) -> list[float]:
        values: list[float] = []
        for frame in self.sample_frames:
            selected = []
            for name, cpu in frame.items():
                if not self._matches_any_prefix(name, self.workload_pg_prefixes):
                    continue
                if self.exclude_pg_prefixes and self._matches_any_prefix(
                    name, self.exclude_pg_prefixes
                ):
                    continue
                selected.append(cpu)
            if selected:
                values.append(sum(selected))
        return values

    @staticmethod
    def _summary(values: list[float]) -> tuple[float | None, float | None]:
        if not values:
            return None, None
        return round(sum(values) / len(values), 1), round(max(values), 1)

    def report(self) -> dict:
        per_container = {}
        for name, values in self.samples.items():
            if values:
                per_container[name] = {
                    "mean_pct": round(sum(values) / len(values), 1),
                    "max_pct": round(max(values), 1),
                    "samples": len(values),
                }

        realtime_samples = self._group_samples(self.realtime_prefixes)
        harness_container_samples = self._group_samples(
            self.harness_container_prefixes
        )
        workload_pg_samples = self._workload_pg_samples()

        # Python-Prozess + ggf. separate Clientcontainer bilden gemeinsam die
        # Messapparatur auf den Harness-Kernen.
        harness_samples: list[float] = []
        n = max(len(self.process_cpu_pct), len(harness_container_samples))
        for i in range(n):
            p = self.process_cpu_pct[i] if i < len(self.process_cpu_pct) else 0.0
            c = (
                harness_container_samples[i]
                if i < len(harness_container_samples)
                else 0.0
            )
            harness_samples.append(p + c)

        rt_mean, rt_max = self._summary(realtime_samples)
        h_mean, h_max = self._summary(harness_samples)
        pg_mean, pg_max = self._summary(workload_pg_samples)

        lag_values = [
            int(x["max_lag_bytes"])
            for x in self.replication_lag_series
            if isinstance(x.get("max_lag_bytes"), int)
        ]

        return {
            "per_container": per_container,
            "realtime_cpu_mean_pct": rt_mean,
            "realtime_cpu_max_pct": rt_max,
            "harness_cpu_mean_pct": h_mean,
            "harness_cpu_max_pct": h_max,
            "python_harness_cpu_mean_pct": (
                round(sum(self.process_cpu_pct) / len(self.process_cpu_pct), 1)
                if self.process_cpu_pct
                else None
            ),
            "python_harness_cpu_max_pct": (
                round(max(self.process_cpu_pct), 1)
                if self.process_cpu_pct
                else None
            ),
            "workload_pg_cpu_mean_pct": pg_mean,
            "workload_pg_cpu_max_pct": pg_max,
            "steal_mean_pct": (
                round(sum(self.steal_pct) / len(self.steal_pct), 3)
                if self.steal_pct
                else None
            ),
            "steal_max_pct": (
                round(max(self.steal_pct), 3) if self.steal_pct else None
            ),
            "replication_lag_start_bytes": lag_values[0] if lag_values else None,
            "replication_lag_end_bytes": lag_values[-1] if lag_values else None,
            "replication_lag_max_bytes": max(lag_values) if lag_values else None,
            "replication_lag_growth_bytes": (
                lag_values[-1] - lag_values[0] if len(lag_values) >= 2 else None
            ),
            "replication_lag_series": self.replication_lag_series,
            "monitor_samples": len(self.sample_frames),
            "monitor_errors": self.errors[:5],
        }
