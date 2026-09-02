#!/usr/bin/env python3
"""
Gemeinsamer Kern des Expressivitaetsharness fuer Hasura und Zero.

Gepruefte Dimensionen:
    e1_and     Composite Query: AND
    e1_or      Composite Query: OR
    e2_sorted  ORDER BY
    e3_limit   ORDER BY + LIMIT
    e4_offset  ORDER BY + LIMIT + OFFSET
    e5_join    Fenster + Relation auf users
    e6_count   Aggregation COUNT
    e7_max     Aggregation MAX
    e8_text    Fenster + ILIKE-Substringfilter

Das Urteil besteht aus zwei getrennten Fragen:
1. Ist die Anfrageklasse ueber die Realtime-Schnittstelle formulierbar?
2. Falls ja: konvergiert der gepflegte Zustand nach gezielten Mutationen
   gegen dieselbe Pull-Anfrage auf PostgreSQL?

Negativproben verwenden eine Fortschrittsbarriere. Ein blosses Ausbleiben
einer Benachrichtigung gilt nicht als Nachweis, weil die Mutation auch noch
unverarbeitet sein koennte.

Hinweis:
e8_text ist ILIKE-Substring-/Pattern-Matching und keine PostgreSQL-
Volltextsuche mit tsvector/tsquery.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional


CLASSES = [
    "e1_and",
    "e1_or",
    "e2_sorted",
    "e3_limit",
    "e4_offset",
    "e5_join",
    "e6_count",
    "e7_max",
    "e8_text",
]

CLASS_LABEL = {
    "e1_and": "Composite Query (AND)",
    "e1_or": "Composite Query (OR)",
    "e2_sorted": "Sorted Query",
    "e3_limit": "Sorted Query + Limit",
    "e4_offset": "Sorted Query + Limit + Offset",
    "e5_join": "Join/Relation (messages -> users)",
    "e6_count": "Aggregation COUNT",
    "e7_max": "Aggregation MAX",
    "e8_text": "Substring-Filter (ILIKE)",
}

WINDOW_CLASSES = {"e3_limit", "e4_offset", "e5_join", "e8_text"}
ORDERED_CLASSES = {"e2_sorted"} | WINDOW_CLASSES
AGG_CLASSES = {"e6_count", "e7_max"}


@dataclass
class Cfg:
    topic: int = 1
    other_topic: int = 2
    user_a: int = 1
    user_b: int = 2
    user_off: int = 7
    needle: str = "gamma"
    miss_word: str = "omega"
    limit: int = 10
    offset: int = 5


# ---------------------------------------------------------------------------
# PostgreSQL-Orakel
# ---------------------------------------------------------------------------

def oracle(conn, cls: str, cfg: Cfg) -> list[dict[str, Any]]:
    """
    Kanonisches Referenzergebnis direkt aus PostgreSQL.

    Zeilenklassen:
        [{"id": int, "author_name": str}, ...]

    Aggregationsklassen:
        [{"value": int|None}]

    created_at wird nicht direkt verglichen. Fuer sortierte Klassen wird die
    vom SQL-Ergebnis vorgegebene ID-Reihenfolge verglichen.
    """
    with conn.cursor() as cur:
        if cls == "e6_count":
            cur.execute(
                "SELECT count(*)::bigint FROM messages WHERE topic_id = %s",
                (cfg.topic,),
            )
            return [{"value": int(cur.fetchone()[0])}]

        if cls == "e7_max":
            cur.execute(
                "SELECT max(id)::bigint FROM messages WHERE topic_id = %s",
                (cfg.topic,),
            )
            value = cur.fetchone()[0]
            return [{"value": None if value is None else int(value)}]

        sql = (
            "SELECT m.id, u.name "
            "FROM messages m "
            "JOIN users u ON u.id = m.user_id "
            "WHERE m.topic_id = %s "
        )
        params: list[Any] = [cfg.topic]

        if cls == "e1_and":
            sql += "AND m.user_id = %s "
            params.append(cfg.user_a)
        elif cls == "e1_or":
            sql += "AND (m.user_id = %s OR m.user_id = %s) "
            params.extend([cfg.user_a, cfg.user_b])
        elif cls == "e8_text":
            sql += "AND m.content ILIKE %s "
            params.append(f"%{cfg.needle}%")

        if cls in ORDERED_CLASSES:
            sql += "ORDER BY m.created_at DESC, m.id DESC "
        else:
            sql += "ORDER BY m.id ASC "

        if cls in WINDOW_CLASSES:
            sql += "LIMIT %s "
            params.append(cfg.limit)

        if cls == "e4_offset":
            sql += "OFFSET %s "
            params.append(cfg.offset)

        cur.execute(sql, params)
        return [
            {"id": int(mid), "author_name": str(author_name)}
            for mid, author_name in cur.fetchall()
        ]


def equal(
    cls: str,
    got: Optional[list[dict[str, Any]]],
    want: list[dict[str, Any]],
) -> tuple[bool, str]:
    if got is None:
        return False, "kein Client-Zustand"

    if cls in AGG_CLASSES:
        g = got[0].get("value") if got else None
        w = want[0].get("value") if want else None
        return g == w, f"client={g} sql={w}"

    got_ids = [int(r["id"]) for r in got]
    want_ids = [int(r["id"]) for r in want]

    if cls in ORDERED_CLASSES:
        if got_ids != want_ids:
            return (
                False,
                f"Reihenfolge/IDs abweichend: "
                f"client={got_ids[:12]} sql={want_ids[:12]}",
            )
    else:
        if sorted(got_ids) != sorted(want_ids):
            return (
                False,
                f"IDs abweichend: "
                f"client={sorted(got_ids)[:12]} sql={sorted(want_ids)[:12]}",
            )

    if cls == "e5_join":
        got_authors = {int(r["id"]): r.get("author_name") for r in got}
        want_authors = {int(r["id"]): r.get("author_name") for r in want}
        if got_authors != want_authors:
            return False, "Join-Attribute weichen vom PostgreSQL-Orakel ab"

    return True, f"{len(want_ids)} Zeilen entsprechen dem Referenzergebnis"


# ---------------------------------------------------------------------------
# Adapter-Schnittstelle
# ---------------------------------------------------------------------------

class NotExpressible(RuntimeError):
    """Kennzeichnet eine beim Registrierungsversuch nicht formulierbare Anfrage."""
    pass


class Adapter:
    name = "abstract"

    def expressible(self, cls: str) -> tuple[bool, str]:
        raise NotImplementedError

    def open(self, cls: str, cfg: Cfg) -> None:
        raise NotImplementedError

    def snapshot(self) -> Optional[list[dict[str, Any]]]:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Mutationen
# ---------------------------------------------------------------------------

def insert_msg(
    conn,
    cfg: Cfg,
    *,
    topic: int,
    user: int,
    offset_s: float,
    word: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO messages "
            "(topic_id, user_id, content, created_at, match_value) "
            "VALUES (%s, %s, %s, now() + %s * interval '1 second', %s) "
            "RETURNING id",
            (topic, user, f"probe {word}", offset_s, 0),
        )
        rid = int(cur.fetchone()[0])
    conn.commit()
    return rid


def delete_msg(conn, rid: int) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM messages WHERE id = %s", (rid,))
    conn.commit()


def move_topic(conn, rid: int, topic: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE messages SET topic_id = %s WHERE id = %s",
            (topic, rid),
        )
    conn.commit()


def set_user(conn, rid: int, user: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE messages SET user_id = %s WHERE id = %s",
            (user, rid),
        )
    conn.commit()


def set_content(conn, rid: int, word: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE messages SET content = %s WHERE id = %s",
            (f"probe {word}", rid),
        )
    conn.commit()


def read_row(conn, rid: int):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, topic_id, user_id, content, created_at, match_value "
            "FROM messages WHERE id = %s",
            (rid,),
        )
        return cur.fetchone()


def write_row(conn, row) -> None:
    """
    Stellt eine geloeschte Seed-Zeile mit identischer ID wieder her.

    Das gemeinsame Schema verwendet eine Identity-Spalte. OVERRIDING SYSTEM
    VALUE macht den Restore unabhaengig davon, ob die Identity als ALWAYS
    oder BY DEFAULT angelegt wurde.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO messages "
            "(id, topic_id, user_id, content, created_at, match_value) "
            "OVERRIDING SYSTEM VALUE "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            row,
        )
    conn.commit()


def set_user_name(conn, uid: int, name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET name = %s WHERE id = %s",
            (name, uid),
        )
    conn.commit()


def get_user_name(conn, uid: int) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM users WHERE id = %s", (uid,))
        row = cur.fetchone()
    return None if row is None else str(row[0])


# ---------------------------------------------------------------------------
# Ergebnisobjekt
# ---------------------------------------------------------------------------

@dataclass
class Result:
    cls: str
    probe: str
    verdict: str          # pass | fail | inconclusive | na
    state_proof: bool
    detail: str
    latency_s: Optional[float] = None


# ---------------------------------------------------------------------------
# Gemeinsamer Runner
# ---------------------------------------------------------------------------

class Runner:
    def __init__(
        self,
        adapter: Adapter,
        conn,
        cfg: Cfg,
        timeout: float = 20.0,
    ):
        self.ad = adapter
        self.conn = conn
        self.cfg = cfg
        self.timeout = timeout
        self.results: list[Result] = []

    def _converge(self, cls: str) -> tuple[bool, str, float]:
        start = time.monotonic()
        deadline = start + self.timeout
        detail = "keine Konvergenz"

        while time.monotonic() < deadline:
            want = oracle(self.conn, cls, self.cfg)
            ok, detail = equal(cls, self.ad.snapshot(), want)
            if ok:
                return True, detail, time.monotonic() - start
            time.sleep(0.1)

        return False, detail, time.monotonic() - start

    def _record(
        self,
        cls: str,
        probe: str,
        verdict: str,
        detail: str,
        state_proof: bool = False,
        latency_s: Optional[float] = None,
    ) -> None:
        self.results.append(
            Result(
                cls=cls,
                probe=probe,
                verdict=verdict,
                state_proof=state_proof,
                detail=detail,
                latency_s=latency_s,
            )
        )
        mark = {
            "pass": "BESTANDEN",
            "fail": "DURCHGEFALLEN",
            "inconclusive": "UNKLAR",
            "na": "N/A",
        }[verdict]
        print(f"  {cls:10s} {probe:36s} {mark:14s} {detail}")

    def positive(
        self,
        cls: str,
        probe: str,
        mutate: Callable[[], None],
        *,
        state_proof: bool = False,
    ) -> None:
        mutate()
        ok, detail, latency = self._converge(cls)
        self._record(
            cls,
            probe,
            "pass" if ok else "fail",
            detail,
            state_proof,
            round(latency, 3),
        )

    def negative(
        self,
        cls: str,
        probe: str,
        mutate: Callable[[], int],
    ) -> None:
        """
        Fuehrt zuerst eine irrelevante Mutation aus und danach eine relevante
        Fortschrittsbarriere.

        Das Bestehen bedeutet nicht, dass die Barrierenzeile selbst zwingend im
        sichtbaren Ergebnis steht (z.B. bei OFFSET). Entscheidend ist, dass der
        nach der Barriere erwartete Zustand erreicht wird.
        """
        irrelevant_id = mutate()

        barrier_id = insert_msg(
            self.conn,
            self.cfg,
            topic=self.cfg.topic,
            user=self.cfg.user_a,
            offset_s=3600,
            word=self.cfg.needle,
        )

        try:
            ok, detail, latency = self._converge(cls)

            if ok:
                self._record(
                    cls,
                    probe,
                    "pass",
                    (
                        f"Fortschrittsbarriere {barrier_id} verarbeitet; "
                        f"gepflegter Zustand entspricht dem Orakel ({detail})"
                    ),
                    False,
                    round(latency, 3),
                )
            else:
                # Bei Zeilenklassen kann ein sichtbarer irrelevanter Datensatz
                # eindeutig als semantischer Fehler klassifiziert werden.
                snapshot = self.ad.snapshot()
                leaked = False
                if cls not in AGG_CLASSES and snapshot is not None and irrelevant_id > 0:
                    leaked = any(
                        int(row.get("id", -1)) == irrelevant_id
                        for row in snapshot
                    )

                if leaked:
                    self._record(
                        cls,
                        probe,
                        "fail",
                        (
                            f"irrelevante Zeile {irrelevant_id} wurde im "
                            f"gepflegten Ergebnis sichtbar"
                        ),
                    )
                else:
                    self._record(
                        cls,
                        probe,
                        "inconclusive",
                        (
                            f"Zustand erreichte das Orakel nach der Barriere "
                            f"nicht: {detail}. Es ist nicht eindeutig, ob die "
                            f"Barriere noch nicht verarbeitet wurde."
                        ),
                    )
        finally:
            # Cleanup darf keine Folgetests verfaelschen.
            try:
                delete_msg(self.conn, barrier_id)
            except Exception:
                self.conn.rollback()

            if irrelevant_id > 0:
                try:
                    delete_msg(self.conn, irrelevant_id)
                except Exception:
                    self.conn.rollback()

            self._converge(cls)

    def run_class(self, cls: str) -> None:
        cfg = self.cfg

        expressible, reason = self.ad.expressible(cls)
        if not expressible:
            self._record(
                cls,
                "gesamte Klasse",
                "na",
                f"nicht formulierbar: {reason}",
            )
            return

        print(f"\n[{self.ad.name}] {cls} - {CLASS_LABEL[cls]}")

        try:
            self.ad.open(cls, cfg)
        except NotExpressible as exc:
            # Die Einstufung stammt aus dem ausgefuehrten Registrierungsversuch.
            self._record(
                cls,
                "T0 Formulierbarkeit",
                "na",
                f"Registrierungsversuch ergab: nicht formulierbar: {exc}",
            )
            try:
                self.ad.close()
            except Exception:
                pass
            return
        except Exception as exc:
            self._record(
                cls,
                "T0 Registrierung",
                "fail",
                f"Live Query konnte nicht registriert werden: {exc}",
            )
            try:
                self.ad.close()
            except Exception:
                pass
            return

        try:
            ok, detail, latency = self._converge(cls)
            self._record(
                cls,
                "T0 Initialzustand",
                "pass" if ok else "fail",
                detail,
                False,
                round(latency, 3),
            )
            if not ok:
                return

            # ---------------------------------------------------------------
            # Fenster-Nachruecker aus zuvor nicht ausgeliefertem Rang
            # ---------------------------------------------------------------
            if cls in WINDOW_CLASSES:
                before = [int(r["id"]) for r in oracle(self.conn, cls, cfg)]

                if len(before) < cfg.limit:
                    self._record(
                        cls,
                        "T1 Unbekannter Rang rueckt nach",
                        "inconclusive",
                        (
                            f"Fenster enthaelt nur {len(before)} Zeilen; "
                            f"kein Nachruecker hinter der sichtbaren Grenze "
                            f"nachweisbar"
                        ),
                    )
                else:
                    victim = before[0]
                    saved = read_row(self.conn, victim)

                    if saved is None:
                        self._record(
                            cls,
                            "T1 Unbekannter Rang rueckt nach",
                            "fail",
                            f"Seed-Zeile {victim} konnte nicht gelesen werden",
                            True,
                        )
                    else:
                        test_recorded = False
                        try:
                            delete_msg(self.conn, victim)
                            conv_ok, conv_detail, conv_latency = self._converge(cls)

                            after = [
                                int(r["id"])
                                for r in oracle(self.conn, cls, cfg)
                            ]
                            promoted = [mid for mid in after if mid not in before]
                            good = (
                                conv_ok
                                and len(promoted) == 1
                                and victim not in after
                            )

                            self._record(
                                cls,
                                "T1 Unbekannter Rang rueckt nach",
                                "pass" if good else "fail",
                                (
                                    f"geloescht={victim}, "
                                    f"nachgerueckt={promoted}; {conv_detail}"
                                ),
                                True,
                                round(conv_latency, 3),
                            )
                            test_recorded = True
                        finally:
                            # Auch bei Exception/Abbruch darf der feste Seed
                            # nicht dauerhaft veraendert bleiben.
                            try:
                                if read_row(self.conn, victim) is None:
                                    write_row(self.conn, saved)
                            except Exception:
                                self.conn.rollback()
                                if not test_recorded:
                                    self._record(
                                        cls,
                                        "T1 Seed-Restore",
                                        "fail",
                                        f"Seed-Zeile {victim} konnte nicht "
                                        f"wiederhergestellt werden",
                                        True,
                                    )
                            self._converge(cls)

            # ---------------------------------------------------------------
            # Eintritt
            # ---------------------------------------------------------------
            hit_user = cfg.user_b if cls == "e1_or" else cfg.user_a
            new_id: Optional[int] = None

            def _insert_hit() -> None:
                nonlocal new_id
                new_id = insert_msg(
                    self.conn,
                    cfg,
                    topic=cfg.topic,
                    user=hit_user,
                    offset_s=60,
                    word=cfg.needle,
                )

            self.positive(
                cls,
                "T2 Insert -> Eintritt",
                _insert_hit,
                state_proof=(cls in WINDOW_CLASSES),
            )

            if new_id is None:
                self._record(
                    cls,
                    "T3-T5 Folgetests",
                    "fail",
                    "Insert-Probe hat keine ID geliefert",
                )
                return

            # ---------------------------------------------------------------
            # Austritt durch Update
            # ---------------------------------------------------------------
            if cls in ("e1_and", "e1_or"):
                self.positive(
                    cls,
                    "T3 Update Filter -> Austritt",
                    lambda: set_user(self.conn, new_id, cfg.user_off),
                )
            elif cls == "e8_text":
                self.positive(
                    cls,
                    "T3 Update Text -> Austritt",
                    lambda: set_content(self.conn, new_id, cfg.miss_word),
                )
            else:
                self.positive(
                    cls,
                    "T3 Update Topic -> Austritt",
                    lambda: move_topic(self.conn, new_id, cfg.other_topic),
                )

            # ---------------------------------------------------------------
            # Wiedereintritt
            # ---------------------------------------------------------------
            if cls in ("e1_and", "e1_or"):
                self.positive(
                    cls,
                    "T4 Update Filter -> Wiedereintritt",
                    lambda: set_user(self.conn, new_id, hit_user),
                )
            elif cls == "e8_text":
                self.positive(
                    cls,
                    "T4 Update Text -> Wiedereintritt",
                    lambda: set_content(self.conn, new_id, cfg.needle),
                )
            else:
                self.positive(
                    cls,
                    "T4 Update Topic -> Wiedereintritt",
                    lambda: move_topic(self.conn, new_id, cfg.topic),
                )

            # ---------------------------------------------------------------
            # Delete
            # ---------------------------------------------------------------
            self.positive(
                cls,
                "T5 Delete -> Austritt",
                lambda: delete_msg(self.conn, new_id),
                state_proof=(cls in WINDOW_CLASSES or cls in AGG_CLASSES),
            )

            # ---------------------------------------------------------------
            # Join-Gegenseite
            # ---------------------------------------------------------------
            if cls == "e5_join":
                rows = oracle(self.conn, cls, cfg)
                if not rows:
                    self._record(
                        cls,
                        "T6 Update der Join-Gegenseite",
                        "inconclusive",
                        "kein Join-Ergebnis vorhanden",
                        True,
                    )
                else:
                    with self.conn.cursor() as cur:
                        cur.execute(
                            "SELECT user_id FROM messages WHERE id = %s",
                            (rows[0]["id"],),
                        )
                        row = cur.fetchone()

                    if row is None:
                        self._record(
                            cls,
                            "T6 Update der Join-Gegenseite",
                            "fail",
                            "zugehoerige Message konnte nicht gelesen werden",
                            True,
                        )
                    else:
                        uid = int(row[0])
                        original = get_user_name(self.conn, uid)

                        if original is None:
                            self._record(
                                cls,
                                "T6 Update der Join-Gegenseite",
                                "fail",
                                f"User {uid} existiert nicht",
                                True,
                            )
                        else:
                            changed = f"{original}__expr_probe__"
                            try:
                                self.positive(
                                    cls,
                                    "T6 Update der Join-Gegenseite",
                                    lambda: set_user_name(
                                        self.conn,
                                        uid,
                                        changed,
                                    ),
                                    state_proof=True,
                                )
                            finally:
                                # Restore auch bei Fehlschlag/Exception.
                                try:
                                    set_user_name(self.conn, uid, original)
                                except Exception:
                                    self.conn.rollback()
                                self._converge(cls)

            # ---------------------------------------------------------------
            # Negativprobe
            # ---------------------------------------------------------------
            def _irrelevant() -> int:
                if cls == "e8_text":
                    return insert_msg(
                        self.conn,
                        cfg,
                        topic=cfg.topic,
                        user=hit_user,
                        offset_s=70,
                        word=cfg.miss_word,
                    )

                if cls in ("e1_and", "e1_or"):
                    return insert_msg(
                        self.conn,
                        cfg,
                        topic=cfg.topic,
                        user=cfg.user_off,
                        offset_s=70,
                        word=cfg.needle,
                    )

                return insert_msg(
                    self.conn,
                    cfg,
                    topic=cfg.other_topic,
                    user=hit_user,
                    offset_s=70,
                    word=cfg.needle,
                )

            self.negative(
                cls,
                "T7 Nichttreffer bleibt draussen",
                _irrelevant,
            )

            # ---------------------------------------------------------------
            # Alte Zeile darf ein volles Fenster nicht betreten
            # ---------------------------------------------------------------
            if cls in WINDOW_CLASSES:
                def _old() -> int:
                    return insert_msg(
                        self.conn,
                        cfg,
                        topic=cfg.topic,
                        user=hit_user,
                        offset_s=-86400 * 365,
                        word=cfg.needle,
                    )

                self.negative(
                    cls,
                    "T8 Alte Zeile betritt Fenster nicht",
                    _old,
                )

        finally:
            self.ad.close()

    def cleanup(self, baseline_id: int) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM messages WHERE id > %s",
                (baseline_id,),
            )
        self.conn.commit()
        print("aufgeraeumt")


# ---------------------------------------------------------------------------
# Gemeinsames Ergebnisformat
# ---------------------------------------------------------------------------

def report(
    system: str,
    cfg: Cfg,
    results: list[Result],
) -> dict[str, Any]:
    by_class: dict[str, Any] = {}

    for cls in CLASSES:
        rows = [r for r in results if r.cls == cls]
        if not rows:
            continue

        if all(r.verdict == "na" for r in rows):
            verdict = "not_expressible"
        elif any(r.verdict == "fail" for r in rows):
            verdict = "unsupported"
        elif any(r.verdict == "inconclusive" for r in rows):
            verdict = "inconclusive"
        else:
            verdict = "supported"

        latencies = [
            float(r.latency_s)
            for r in rows
            if r.latency_s is not None
        ]

        by_class[cls] = {
            "label": CLASS_LABEL[cls],
            "verdict": verdict,
            "probes": [r.__dict__ for r in rows],
            "state_proofs_passed": sum(
                1
                for r in rows
                if r.state_proof and r.verdict == "pass"
            ),
            "median_converge_s": (
                round(sorted(latencies)[len(latencies) // 2], 3)
                if latencies
                else None
            ),
        }

    return {
        "system": system,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "harness": (
            "expr_core v3 "
            "(PostgreSQL oracle + progress barrier + empirical capability probe)"
        ),
        "semantic_scope": (
            "initial_result_plus_continuous_result_maintenance"
        ),
        "negative_probe_method": "progress_barrier",
        "topology": cfg.__dict__,
        "text_filter_note": (
            "e8_text uses ILIKE substring/pattern matching; "
            "it is not PostgreSQL full-text search"
        ),
        "classes": by_class,
    }


def write_report(path: str, data: dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    print(f"-> {path}")


def matrix(paths: list[str], out: str) -> None:
    reports = []
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            reports.append(json.load(fh))

    symbols = {
        "supported": "ja",
        "unsupported": "nein",
        "not_expressible": "nicht formulierbar",
        "inconclusive": "unklar",
    }

    lines = [
        "| Dimension | "
        + " | ".join(r["system"] for r in reports)
        + " |",
        "|---|" + "---|" * len(reports),
    ]

    for cls in CLASSES:
        cells = []
        for rep in reports:
            item = rep["classes"].get(cls)
            if item is None:
                cells.append("nicht gemessen")
                continue

            cell = symbols[item["verdict"]]
            if item["state_proofs_passed"]:
                cell += (
                    f" ({item['state_proofs_passed']} Zustandsproben)"
                )
            cells.append(cell)

        lines.append(
            f"| {CLASS_LABEL[cls]} | "
            + " | ".join(cells)
            + " |"
        )

    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    print("\n".join(lines))
    print(f"\n-> {out}")
