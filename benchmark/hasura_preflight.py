"""Vorbedingungen fuer einen finalen Hasura-Messlauf."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2
import requests


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
    ap.add_argument("--expect-messages", type=int, default=1000000)
    ap.add_argument("--expect-topics", type=int, default=30000)
    ap.add_argument("--expect-users", type=int, default=1000)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    repo = Path(__file__).resolve().parents[1]
    env = {**load_dotenv(repo / ".env"), **os.environ}
    problems: list[str] = []
    report: dict[str, object] = {}

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=int(env.get("POSTGRES_PORT", "5432")),
        user=env.get("POSTGRES_USER", "postgres"),
        password=env.get("POSTGRES_PASSWORD", "postgres"),
        dbname=env.get("POSTGRES_DB", "postgres"),
    )
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              (SELECT count(*) FROM topics),
              (SELECT count(*) FROM users),
              (SELECT count(*) FROM messages)
            """
        )
        topics, users, messages = map(int, cur.fetchone())
        report.update(
            {
                "topics": topics,
                "users": users,
                "messages": messages,
                "messages_per_topic": round(messages / topics, 2) if topics else None,
            }
        )
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='messages'
                  AND column_name='match_value'
            )
            """
        )
        report["match_value_present"] = bool(cur.fetchone()[0])
    conn.close()

    if topics != args.expect_topics:
        problems.append(f"topics={topics}, erwartet {args.expect_topics}")
    if users != args.expect_users:
        problems.append(f"users={users}, erwartet {args.expect_users}")
    if messages < args.expect_messages:
        problems.append(f"messages={messages}, erwartet mindestens {args.expect_messages}")
    if not report["match_value_present"]:
        problems.append("messages.match_value fehlt; gemeinsames Schema noch nicht aktiv")

    port = env.get("HASURA_PORT", "8080")
    secret = env.get("HASURA_ADMIN_SECRET", "")
    headers = {"content-type": "application/json"}
    if secret:
        headers["x-hasura-admin-secret"] = secret

    try:
        r = requests.get(f"http://127.0.0.1:{port}/healthz", timeout=5)
        report["hasura_health"] = r.status_code
        if r.status_code >= 300:
            problems.append(f"Hasura healthz={r.status_code}")
    except Exception as exc:  # noqa: BLE001
        report["hasura_health"] = f"unreachable: {exc}"
        problems.append("Hasura nicht erreichbar")

    # Kleine Query prueft Tracking + author-Relationship.
    gql = {
        "query": "query { messages(limit: 1) { id author { id name } } }"
    }
    try:
        r = requests.post(
            f"http://127.0.0.1:{port}/v1/graphql",
            headers=headers,
            json=gql,
            timeout=10,
        )
        payload = r.json()
        report["graphql_probe"] = payload
        if r.status_code >= 300 or payload.get("errors"):
            problems.append("Hasura public.messages/author nicht korrekt getrackt")
    except Exception as exc:  # noqa: BLE001
        report["graphql_probe"] = f"failed: {exc}"
        problems.append("Hasura GraphQL-Probe fehlgeschlagen")

    report["problems"] = problems
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(
            f"topics={topics} users={users} messages={messages} "
            f"messages_per_topic={report['messages_per_topic']}"
        )
        print(f"match_value_present={report['match_value_present']}")
        print(f"hasura_health={report.get('hasura_health')}")
        for p in problems:
            print(f"FAIL: {p}")

    if problems:
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
