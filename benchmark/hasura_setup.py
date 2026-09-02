"""Idempotentes Hasura-Metadata-Setup fuer das gemeinsame public-Schema."""

from __future__ import annotations

import json
import os
from pathlib import Path

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
    repo = Path(__file__).resolve().parents[1]
    env = {**load_dotenv(repo / ".env"), **os.environ}
    port = env.get("HASURA_PORT", "8080")
    secret = env.get("HASURA_ADMIN_SECRET", "")
    source = env.get("HASURA_SOURCE", "default")

    headers = {"content-type": "application/json"}
    if secret:
        headers["x-hasura-admin-secret"] = secret

    base = f"http://127.0.0.1:{port}"

    health = requests.get(f"{base}/healthz", timeout=10)
    health.raise_for_status()

    def meta(payload: dict, *, tolerate_already: bool = True) -> None:
        r = requests.post(
            f"{base}/v1/metadata",
            headers=headers,
            json=payload,
            timeout=30,
        )
        if r.status_code < 300:
            return
        text = r.text.lower()
        if tolerate_already and (
            "already" in text
            or "exists" in text
            or "tracked" in text
        ):
            return
        raise RuntimeError(
            f"Hasura metadata error {r.status_code}: {r.text[:500]}"
        )

    for table_name in ("users", "topics", "messages"):
        meta(
            {
                "type": "pg_track_table",
                "args": {
                    "source": source,
                    "table": {"schema": "public", "name": table_name},
                },
            }
        )

    # Gleicher semantischer Name wie in Zero: message.author -> users.
    meta(
        {
            "type": "pg_create_object_relationship",
            "args": {
                "source": source,
                "table": {"schema": "public", "name": "messages"},
                "name": "author",
                "using": {"foreign_key_constraint_on": "user_id"},
            },
        }
    )

    meta({"type": "reload_metadata", "args": {}})

    print(
        json.dumps(
            {
                "hasura": "ready",
                "tracked_tables": ["users", "topics", "messages"],
                "messages_relationship": "author",
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
