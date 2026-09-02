#!/usr/bin/env python3
"""
Empirischer Expressivitaetstest fuer Zero.

Ablauf je Klasse:
1. Der Adapter versucht, die passende Zero-Query gegen die laufende Zero-
   Installation zu registrieren/materialisieren.
2. Wird die Query erfolgreich registriert, prueft expr_core den initialen
   Zustand und die kontinuierliche Ergebnispflege gegen PostgreSQL.
3. Ein beim Registrierungsversuch festgestellter fehlender ZQL-Operator wird
   als `not_expressible` ausgewertet.
4. Andere Fehler werden als technische oder semantische Fehler gespeichert.

Die Formulierbarkeit wird gegen die installierte Zero-Version geprueft. Die
bestehende Relation `author` wird fuer den Join verwendet.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any, Optional

import psycopg2

import expr_core as core
from expr_core import Adapter, Cfg, NotExpressible, Runner
from zero_adapter import ZeroSubscriberPool


QUERY_INDEX = 0
NOT_EXPRESSIBLE_MARKER = "__NOT_EXPRESSIBLE__:"


QUERY_CLASS = {
    "e1_and": "composite_and",
    "e1_or": "composite_or",
    "e2_sorted": "sorted",
    "e3_limit": "window",
    "e4_offset": "offset_probe",
    "e5_join": "window_join",
    "e6_count": "count_probe",
    "e7_max": "max_probe",
    "e8_text": "window_search",
}


def _extract_not_expressible(errors: list[dict[str, Any]]) -> Optional[str]:
    """Extrahiert explizit markierte Fehler zur Formulierbarkeit."""
    blob = json.dumps(errors, ensure_ascii=False, default=str)
    pos = blob.find(NOT_EXPRESSIBLE_MARKER)
    if pos < 0:
        return None

    text = blob[pos + len(NOT_EXPRESSIBLE_MARKER):]
    # Fuer die Auswertung genuegt die erste Fehlermeldung.
    for sep in ("\\\\n", "\\n", '"}', '",', '"'):
        if sep in text:
            text = text.split(sep, 1)[0]
    return text.strip(" :\\\\\"\'") or "Operator im ausgefuehrten ZQL-Objekt nicht vorhanden"


class ZeroAdapter(Adapter):
    name = "zero"

    def __init__(
        self,
        repo: Path,
        loop: asyncio.AbstractEventLoop,
        topics: int,
    ):
        self.repo = repo
        self.loop = loop
        self.topics = topics
        self.pool: Optional[ZeroSubscriberPool] = None
        self.cls = ""

    def expressible(self, cls: str) -> tuple[bool, str]:
        # Jede implementierte Katalogklasse wird registriert und geprueft.
        if cls not in QUERY_CLASS:
            return False, "keine Test-Query fuer diese Katalogklasse implementiert"
        return True, ""

    def open(self, cls: str, cfg: Cfg) -> None:
        self.cls = cls
        self.pool = ZeroSubscriberPool(
            repo_root=self.repo,
            total_queries=1,
            clients=1,
            query_class=QUERY_CLASS[cls],
            topics=self.topics,
            limit=cfg.limit,
            offset=cfg.offset,
            needle=cfg.needle,
            emit_rows=True,
        )

        self.loop.run_until_complete(self.pool.start())

        try:
            self.loop.run_until_complete(
                self.pool.wait_ready(timeout=300)
            )
        except Exception as exc:
            # Einige Operatorfehler treten bereits bei queryFor()/materialize()
            # auf. Die Ereignisschleife liest die Fehlermeldung aus stderr ein.
            self.loop.run_until_complete(asyncio.sleep(0.10))

            evidence: list[Any] = list(self.pool.errors)
            evidence.extend({"log": line} for line in list(self.pool.logs))

            reason = _extract_not_expressible(evidence)
            if reason is not None:
                raise NotExpressible(reason) from exc

            # Ohne Markierung bleibt der Fehler als technischer Fehler erhalten.
            raise

    def snapshot(self) -> Optional[list[dict[str, Any]]]:
        if self.pool is None:
            return None

        # Der synchrone Runner gibt der Ereignisschleife Zeit, Ausgaben der
        # Node-Clients einzulesen. Dieser Pfad gehoert nur zum Korrektheitstest.
        self.loop.run_until_complete(asyncio.sleep(0.02))

        rows = self.pool.row_snapshots.get(QUERY_INDEX)
        if rows is None:
            return None

        # Kanonische Ausgabe fuer Zeilen- und Join-Klassen.
        out: list[dict[str, Any]] = []
        for row in rows:
            if "value" in row:
                # Kanonische Ausgabe fuer materialisierbare Aggregatabfragen.
                out.append({"value": row.get("value")})
                continue

            author = row.get("author") or {}
            out.append(
                {
                    "id": int(row.get("id", 0)),
                    "author_name": author.get("name"),
                }
            )
        return out

    def close(self) -> None:
        if self.pool is not None:
            self.loop.run_until_complete(self.pool.close())
            self.pool = None


def load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out

    for raw in path.read_text(encoding="utf-8").splitlines():
        raw = raw.strip()
        if raw and not raw.startswith("#") and "=" in raw:
            key, value = raw.split("=", 1)
            out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--classes", default="all")
    ap.add_argument("--topic", type=int, default=1)
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--offset", type=int, default=5)
    ap.add_argument("--needle", default="gamma")
    ap.add_argument("--topics", type=int, default=30000)
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument(
        "--json-out",
        default="results/expr_zero.json",
    )
    args = ap.parse_args()

    classes = (
        core.CLASSES
        if args.classes == "all"
        else [x.strip() for x in args.classes.split(",") if x.strip()]
    )

    unknown = [c for c in classes if c not in core.CLASSES]
    if unknown:
        raise SystemExit(f"unbekannte Klassen: {unknown}")

    repo = Path(__file__).resolve().parents[1]
    env = {**load_dotenv(repo / ".env"), **os.environ}

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=int(env.get("POSTGRES_PORT", "5432")),
        user=env.get("POSTGRES_USER", "postgres"),
        password=env.get("POSTGRES_PASSWORD", "postgres"),
        dbname=env.get("POSTGRES_DB", "postgres"),
    )
    conn.autocommit = False

    cfg = Cfg(
        topic=args.topic,
        limit=args.limit,
        offset=args.offset,
        needle=args.needle,
    )

    with conn.cursor() as cur:
        cur.execute("SELECT coalesce(max(id), 0) FROM messages")
        baseline = int(cur.fetchone()[0])

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    adapter = ZeroAdapter(
        repo=repo,
        loop=loop,
        topics=args.topics,
    )
    runner = Runner(
        adapter,
        conn,
        cfg,
        timeout=args.timeout,
    )

    try:
        for cls in classes:
            runner.run_class(cls)
    finally:
        try:
            runner.cleanup(baseline)
        finally:
            loop.close()

    data = core.report("zero", cfg, runner.results)

    # Dokumentiert explizit, dass fehlende Operatoren erst aus einem
    # ausgefuehrten Registrierungsversuch abgeleitet werden.
    data["capability_method"] = (
        "runtime_registration_probe_against_installed_zero"
    )
    data["capability_marker"] = NOT_EXPRESSIBLE_MARKER

    out_path = Path(args.json_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    core.write_report(str(out_path), data)

    conn.close()

    # not_expressible ist ein fachliches Ergebnis, kein Harness-Fehler.
    # Nur echte FAIL-Proben liefern Exitcode 1.
    return 1 if any(
        result.verdict == "fail"
        for result in runner.results
    ) else 0


if __name__ == "__main__":
    raise SystemExit(main())
