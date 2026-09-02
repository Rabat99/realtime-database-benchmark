"""
Vorbedingungen fuer einen Zero-Messlauf.

Der Replikations-Lag zeigt, ob die initiale Uebernahme in die Zero-Replica
abgeschlossen ist. Messungen werden nur ohne relevanten Backlog gestartet.

Die Keyword-Anteile werden gemessen und ausgegeben. Durch die Blockstruktur
des Seeds entsprechen sie nur dann exakt 10/20/50/20, wenn die Zeilenzahl pro
Topic ein Vielfaches von zehn ist.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--max-lag-bytes",
        type=int,
        default=8 * 1024 * 1024,
        help="maximal erlaubter Rueckstand des Zero-Slots",
    )
    ap.add_argument("--expect-messages", type=int, default=1)
    ap.add_argument("--expect-topics", type=int, default=0)
    ap.add_argument("--expect-users", type=int, default=0)
    ap.add_argument(
        "--max-cvr-mb", type=int, default=200,
        help="Warnschwelle fuer die Groesse der CVR-Datenbank",
    )
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    repo = Path(__file__).resolve().parents[1]
    env = {**load_dotenv(repo / ".env"), **os.environ}

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=int(env.get("POSTGRES_PORT", "5432")),
        user=env.get("POSTGRES_USER", "postgres"),
        password=env.get("POSTGRES_PASSWORD", "postgres"),
        dbname=env.get("POSTGRES_DB", "postgres"),
    )

    report: dict[str, object] = {}
    problems: list[str] = []

    with conn, conn.cursor() as cur:
        cur.execute("SHOW wal_level")
        report["wal_level"] = cur.fetchone()[0]

        cur.execute("""
            SELECT
              (SELECT count(*) FROM topics),
              (SELECT count(*) FROM users),
              (SELECT count(*) FROM messages)
        """)
        topics, users, messages = cur.fetchone()
        report["topics"] = topics
        report["users"] = users
        report["messages"] = messages
        report["messages_per_topic"] = (
            round(messages / topics, 2) if topics else None
        )

        cur.execute("""
            SELECT
              count(*) FILTER (WHERE content LIKE '%alpha%'),
              count(*) FILTER (WHERE content LIKE '%beta%'),
              count(*) FILTER (WHERE content LIKE '%gamma%'),
              count(*) FILTER (WHERE content LIKE '%delta%')
            FROM messages
        """)
        alpha, beta, gamma, delta = cur.fetchone()
        total = max(int(messages), 1)
        report["keywords"] = {
            "alpha": alpha, "beta": beta, "gamma": gamma, "delta": delta,
        }
        report["keyword_share"] = {
            "alpha": round(alpha / total, 4),
            "beta": round(beta / total, 4),
            "gamma": round(gamma / total, 4),
            "delta": round(delta / total, 4),
        }

        cur.execute("""
            SELECT
              slot_name,
              active,
              coalesce(
                pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), -1
              )::bigint AS lag_bytes
            FROM pg_replication_slots
            WHERE slot_type = 'logical'
            ORDER BY slot_name
        """)
        slots = [
            {"slot_name": r[0], "active": r[1], "lag_bytes": int(r[2])}
            for r in cur.fetchall()
        ]
        report["slots"] = slots

    # Persistenter Zero-Zustand. Waechst ueber Laeufe hinweg und ueberlebt
    # einen Neustart von zero-cache; ab einer gewissen Groesse steigt die
    # Leerlauflatenz messbar an.
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
            cvr_bytes = int(cur.fetchone()[0])
        meta.close()
        report["cvr_mb"] = round(cvr_bytes / 1024 / 1024, 1)
        if cvr_bytes > args.max_cvr_mb * 1024 * 1024:
            problems.append(
                f"CVR-Datenbank ist {report['cvr_mb']} MB gross "
                f"(Schwelle {args.max_cvr_mb} MB). Zustand aus frueheren "
                "Laeufen vor der Messreihe zuruecksetzen"
            )
    except Exception as exc:  # noqa: BLE001
        report["cvr_mb"] = f"unavailable: {exc}"

    # zero-cache erreichbar?
    zero_port = env.get("ZERO_PORT", "4848")
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{zero_port}/keepalive", timeout=5
        ) as resp:
            report["zero_cache_http"] = resp.status
    except (urllib.error.URLError, OSError) as exc:
        report["zero_cache_http"] = f"unreachable: {exc}"
        problems.append("zero-cache antwortet nicht auf /keepalive")

    if report["wal_level"] != "logical":
        problems.append("wal_level muss logical sein")
    if int(messages) < args.expect_messages:
        problems.append(
            f"zu wenige messages: {messages} < {args.expect_messages}"
        )
    if args.expect_topics and int(topics) != args.expect_topics:
        problems.append(
            f"topics={topics}, erwartet {args.expect_topics}; "
            "Makefile und Seed stimmen nicht ueberein"
        )
    if args.expect_users and int(users) != args.expect_users:
        problems.append(f"users={users}, erwartet {args.expect_users}")
    if not slots:
        problems.append(
            "kein logischer Replication Slot vorhanden, "
            "zero-cache hat die Replikation nicht aufgesetzt"
        )
    for slot in slots:
        if not slot["active"]:
            problems.append(f"Slot {slot['slot_name']} ist inaktiv")
        if slot["lag_bytes"] > args.max_lag_bytes:
            problems.append(
                f"Slot {slot['slot_name']} haengt {slot['lag_bytes']} Bytes "
                f"zurueck (Grenze {args.max_lag_bytes}); Replikation noch nicht "
                "eingeholt"
            )

    if args.json:
        report["problems"] = problems
        print(json.dumps(report, indent=2))
    else:
        print(f"wal_level={report['wal_level']}")
        print(
            f"topics={topics} users={users} messages={messages} "
            f"messages_per_topic={report['messages_per_topic']}"
        )
        print(f"keywords={report['keywords']}")
        print(f"keyword_share={report['keyword_share']}")
        for slot in slots:
            print(
                f"slot={slot['slot_name']} active={slot['active']} "
                f"lag_bytes={slot['lag_bytes']}"
            )
        print(f"cvr_mb={report['cvr_mb']}")
        print(f"zero_cache_http={report['zero_cache_http']}")
        for p in problems:
            print(f"FAIL: {p}")

    if problems:
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
