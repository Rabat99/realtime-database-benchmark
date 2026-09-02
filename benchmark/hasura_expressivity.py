#!/usr/bin/env python3
"""
Expressivitaetstest fuer Hasura Live Queries.

Der Test verwendet das bestehende public-Schema messages/topics/users.
Es wird KEINE neue Relationship angelegt.

Fuer die Join-Klasse wird die bereits in Hasura vorhandene Object-Relationship
von messages auf users verwendet:
- zuerst HASURA_REL, falls als Umgebungsvariable gesetzt,
- sonst automatische Erkennung aus der Hasura-Metadata,
- bevorzugt bestehende Namen `author` oder `user`.

Falls keine passende Relationship existiert, wird nur die Join-Klasse als
"nicht formulierbar" ausgewiesen. Die uebrigen Klassen laufen trotzdem.

Aufruf:
    python hasura_expressivity.py \
      --classes all \
      --json-out results/expr_hasura.json
"""

from __future__ import annotations

import argparse
import json
import os
import threading
from pathlib import Path
from typing import Any, Optional

import psycopg2
import requests
import websocket

import expr_core as core
from expr_core import Adapter, Cfg, Runner


HASURA_HTTP = os.environ.get(
    "HASURA_HTTP",
    "http://127.0.0.1:8080",
)
HASURA_WS = os.environ.get(
    "HASURA_WS",
    "ws://127.0.0.1:8080/v1/graphql",
)
HASURA_SECRET = os.environ.get(
    "HASURA_ADMIN_SECRET",
    "",
)
HASURA_SOURCE = os.environ.get(
    "HASURA_SOURCE",
    "default",
)
ACK_TIMEOUT = float(
    os.environ.get(
        "HASURA_ACK_TIMEOUT_S",
        "20",
    )
)


def admin_headers() -> dict[str, str]:
    headers = {"content-type": "application/json"}
    if HASURA_SECRET:
        headers["x-hasura-admin-secret"] = HASURA_SECRET
    return headers


def detect_existing_relationship() -> Optional[str]:
    """
    Liest die Hasura-Metadata und benutzt eine bereits vorhandene
    Object-Relationship von public.messages.

    Es wird keine neue Relationship erzeugt.
    """
    forced = os.environ.get("HASURA_REL", "").strip()
    if forced:
        return forced

    try:
        response = requests.post(
            f"{HASURA_HTTP}/v1/metadata",
            headers=admin_headers(),
            json={
                "type": "export_metadata",
                "args": {},
            },
            timeout=15,
        )
        response.raise_for_status()
        metadata = response.json()
    except Exception:
        return None

    candidates: list[str] = []

    for source in metadata.get("sources", []):
        if source.get("name") != HASURA_SOURCE:
            continue

        for table_entry in source.get("tables", []):
            table = table_entry.get("table") or {}
            if (
                table.get("schema") == "public"
                and table.get("name") == "messages"
            ):
                for rel in table_entry.get("object_relationships", []):
                    name = rel.get("name")
                    if isinstance(name, str) and name:
                        candidates.append(name)

    for preferred in ("author", "user"):
        if preferred in candidates:
            return preferred

    if len(candidates) == 1:
        return candidates[0]

    return None


def build_query(
    cls: str,
    cfg: Cfg,
    relation_name: Optional[str],
) -> tuple[str, str]:
    """
    Liefert (GraphQL-Dokument, Root-Feld).
    """
    order = "order_by: [{created_at: desc}, {id: desc}]"

    if cls == "e6_count":
        return (
            "subscription { "
            "messages_aggregate("
            f"where: {{topic_id: {{_eq: {cfg.topic}}}}}"
            ") { aggregate { count } } "
            "}",
            "messages_aggregate",
        )

    if cls == "e7_max":
        return (
            "subscription { "
            "messages_aggregate("
            f"where: {{topic_id: {{_eq: {cfg.topic}}}}}"
            ") { aggregate { max { id } } } "
            "}",
            "messages_aggregate",
        )

    if cls == "e1_and":
        where = (
            "{"
            f"topic_id: {{_eq: {cfg.topic}}}, "
            f"user_id: {{_eq: {cfg.user_a}}}"
            "}"
        )
        tail = "order_by: {id: asc}"

    elif cls == "e1_or":
        where = (
            "{"
            f"topic_id: {{_eq: {cfg.topic}}}, "
            "_or: ["
            f"{{user_id: {{_eq: {cfg.user_a}}}}}, "
            f"{{user_id: {{_eq: {cfg.user_b}}}}}"
            "]"
            "}"
        )
        tail = "order_by: {id: asc}"

    elif cls == "e8_text":
        where = (
            "{"
            f"topic_id: {{_eq: {cfg.topic}}}, "
            f'content: {{_ilike: "%{cfg.needle}%"}}'
            "}"
        )
        tail = f"{order}, limit: {cfg.limit}"

    else:
        where = f"{{topic_id: {{_eq: {cfg.topic}}}}}"

        if cls == "e2_sorted":
            tail = order
        elif cls == "e4_offset":
            tail = (
                f"{order}, "
                f"limit: {cfg.limit}, "
                f"offset: {cfg.offset}"
            )
        else:
            # e3_limit / e5_join
            tail = f"{order}, limit: {cfg.limit}"

    if cls == "e5_join":
        if not relation_name:
            raise RuntimeError(
                "keine bestehende messages->users Object-Relationship gefunden"
            )
        fields = (
            "id "
            f"{relation_name} {{ id name }}"
        )
    else:
        fields = "id"

    return (
        "subscription { "
        f"messages(where: {where}, {tail}) "
        f"{{ {fields} }} "
        "}",
        "messages",
    )


class HasuraAdapter(Adapter):
    name = "hasura"

    def __init__(self):
        self.relation_name = detect_existing_relationship()
        self.ws: Optional[websocket.WebSocketApp] = None
        self.cls = ""
        self._root = ""
        self._state: Optional[list[dict[str, Any]]] = None
        self._lock = threading.Lock()
        self._ack = threading.Event()
        self._first_result = threading.Event()
        self._error: Optional[str] = None

        if self.relation_name:
            print(
                f"Hasura: bestehende Relationship fuer Join: "
                f"{self.relation_name}"
            )
        else:
            print(
                "Hasura: keine bestehende messages->users Relationship "
                "gefunden; nur e5_join wird als nicht formulierbar markiert."
            )

    def expressible(self, cls: str) -> tuple[bool, str]:
        if cls == "e5_join" and not self.relation_name:
            return (
                False,
                "keine bereits konfigurierte messages->users "
                "Object-Relationship gefunden",
            )
        return True, ""

    def open(self, cls: str, cfg: Cfg) -> None:
        self.cls = cls
        self._state = None
        self._error = None
        self._ack = threading.Event()
        self._first_result = threading.Event()

        document, root = build_query(
            cls,
            cfg,
            self.relation_name,
        )
        self._root = root

        headers = {}
        if HASURA_SECRET:
            headers["x-hasura-admin-secret"] = HASURA_SECRET

        start_message = json.dumps(
            {
                "id": "1",
                "type": "start",
                "payload": {
                    "query": document,
                    "variables": {},
                },
            }
        )

        def on_open(ws):
            ws.send(
                json.dumps(
                    {
                        "type": "connection_init",
                        "payload": {
                            "headers": headers,
                        },
                    }
                )
            )

        def on_message(ws, raw):
            msg = json.loads(raw)
            typ = msg.get("type")

            if typ == "connection_ack":
                ws.send(start_message)
                self._ack.set()
                return

            if typ in ("data", "next"):
                payload = msg.get("payload") or {}

                if payload.get("errors"):
                    self._error = str(payload["errors"])[:500]
                    self._first_result.set()
                    return

                data = payload.get("data") or {}
                try:
                    canonical = self._canonical(
                        data.get(self._root)
                    )
                except Exception as exc:
                    self._error = (
                        f"Payload konnte nicht normalisiert werden: {exc}"
                    )
                    self._first_result.set()
                    return

                with self._lock:
                    self._state = canonical
                self._first_result.set()
                return

            if typ in ("error", "connection_error"):
                self._error = str(msg)[:500]
                self._first_result.set()

        def on_error(_ws, error):
            self._error = str(error)[:500]
            self._first_result.set()

        self.ws = websocket.WebSocketApp(
            HASURA_WS,
            subprotocols=["graphql-ws"],
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
        )

        threading.Thread(
            target=self.ws.run_forever,
            daemon=True,
        ).start()

        if not self._ack.wait(ACK_TIMEOUT):
            raise RuntimeError(
                f"keine connection_ack nach {ACK_TIMEOUT}s"
            )

        # Registrierung soll nicht erst nach dem vollen Runner-Timeout als
        # Fehler auffallen. Der erste Snapshot oder eine GraphQL-Fehlermeldung
        # muss zeitnah eintreffen.
        if not self._first_result.wait(ACK_TIMEOUT):
            raise RuntimeError(
                f"kein erstes Live-Query-Ergebnis nach weiteren "
                f"{ACK_TIMEOUT}s"
            )

        if self._error:
            raise RuntimeError(self._error)

    def _canonical(
        self,
        node,
    ) -> Optional[list[dict[str, Any]]]:
        if node is None:
            return None

        if self.cls == "e6_count":
            return [
                {
                    "value": int(
                        node["aggregate"]["count"]
                    )
                }
            ]

        if self.cls == "e7_max":
            value = node["aggregate"]["max"]["id"]
            return [
                {
                    "value": (
                        None
                        if value is None
                        else int(value)
                    )
                }
            ]

        out = []

        for row in node:
            author_name = None

            if self.cls == "e5_join":
                relation = (
                    row.get(self.relation_name)
                    if self.relation_name
                    else None
                )
                relation = relation or {}
                author_name = relation.get("name")

            out.append(
                {
                    "id": int(row["id"]),
                    "author_name": author_name,
                }
            )

        return out

    def snapshot(self) -> Optional[list[dict[str, Any]]]:
        with self._lock:
            return (
                None
                if self._state is None
                else [dict(row) for row in self._state]
            )

    def close(self) -> None:
        try:
            if self.ws is not None:
                self.ws.send(
                    json.dumps(
                        {
                            "id": "1",
                            "type": "stop",
                        }
                    )
                )
                self.ws.close()
        except Exception:
            pass
        finally:
            self.ws = None


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
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument(
        "--json-out",
        default="results/expr_hasura.json",
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

    # .env-Werte auch fuer die Modulkonfiguration verfuegbar machen,
    # falls sie nicht bereits exportiert wurden.
    for key, value in env.items():
        os.environ.setdefault(key, value)

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

    runner = Runner(
        HasuraAdapter(),
        conn,
        cfg,
        timeout=args.timeout,
    )

    try:
        for cls in classes:
            runner.run_class(cls)
    finally:
        runner.cleanup(baseline)

    data = core.report(
        "hasura",
        cfg,
        runner.results,
    )

    out_path = Path(args.json_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    core.write_report(str(out_path), data)

    conn.close()

    return 1 if any(
        result.verdict == "fail"
        for result in runner.results
    ) else 0


if __name__ == "__main__":
    raise SystemExit(main())
