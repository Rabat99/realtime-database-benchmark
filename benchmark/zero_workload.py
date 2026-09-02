"""
Konstruktion der Schreiblast fuer den Zero-Benchmark.

Zentrale Invariante: jede geschriebene Zeile trifft genau eine aktive Query
(Selektivitaetskontrolle nach Wingerath). Ohne diese Zuordnung laufen neue
Zeilen an den Subscriptions vorbei, weil der Seed match_value = id * 1000
vergibt und neue ids ausserhalb aller Range-Fenster landen.
"""

from __future__ import annotations

import math
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable

# Anfragetreppe. Jede Stufe fuegt genau eine Eigenschaft hinzu.
# window ist die Kernklasse des Performance-Benchmarks, weil ORDER BY + LIMIT
# eine anspruchsvollere, begrenzte Ergebnispflege erzeugt. Die Klasse dient
# nicht als Definition dafuer, ob etwas eine Live Query ist; auch filter kann
# bei initialem Ergebnis und fortlaufender Pflege eine Live Query darstellen.
QUERY_CLASSES = ("filter", "sorted", "window", "window_join", "window_search")

# Klassen mit gepflegtem Fenster. Nur dort ist die Position einer Zeile in der
# Sortierung entscheidend dafuer, ob sie im Ergebnis erscheint.
WINDOW_CLASSES = ("window", "window_join", "window_search")

# Klasse mit textbasierter ILIKE-Bedingung. Das ist keine PostgreSQL-FTS.
SEARCH_CLASSES = ("window_search",)

# Nominelle Keyword-Verteilung des Seeds. Die tatsaechlichen Anteile haengen
# davon ab, ob die Zeilen pro Topic ein Vielfaches von zehn sind. Der Preflight
# gibt deshalb zusaetzlich die gemessenen Anteile aus.
NOMINAL_NEEDLE_SHARE = {"alpha": 0.10, "beta": 0.20, "gamma": 0.50, "delta": 0.20}

# Wort, das in keiner Suchbedingung vorkommt und damit keine Query trifft.
MISS_KEYWORD = "omega"


def pctl(values: list[float], p: float) -> float | None:
    """Berechnet ein Perzentil nach dem Nearest-Rank-Verfahren."""
    if not values:
        return None
    s = sorted(values)
    k = max(0, math.ceil(p / 100.0 * len(s)) - 1)
    return s[k]


@dataclass
class WorkloadSpec:
    query_class: str
    total_queries: int
    topics: int = 30000
    users: int = 1000
    needle: str = "gamma"

    def __post_init__(self) -> None:
        if self.query_class not in QUERY_CLASSES:
            raise ValueError(
                f"unbekannte query_class {self.query_class!r}, "
                f"erlaubt: {QUERY_CLASSES}"
            )
        if self.total_queries > self.topics:
            raise ValueError(
                f"{self.total_queries} Queries brauchen ebenso viele Topics, "
                f"vorhanden sind {self.topics}"
            )

    # -- Zuordnung Query-Index -> Zeile ------------------------------------

    def topic_for(self, index: int) -> int:
        # Jede Anfrage adressiert genau ein Topic, ueber alle Klassen hinweg.
        return index + 1

    def user_for(self, index: int) -> int:
        return (index % self.users) + 1

    def match_value_for(self, index: int) -> int:
        # Die Spalte ist NOT NULL. Der deterministische Wert erlaubt die
        # Zuordnung der Zeilen zu einem Lauf.
        return (index + 1) * 1000

    def content_for(
        self, index: int, *, token: str | None = None, hit: bool = True
    ) -> str:
        # Ausserhalb der ILIKE-Textfilterklasse ist die Trefferrate bedeutungslos; das
        # Keyword bleibt trotzdem im content, damit die Zeilenlaenge ueber alle
        # Klassen gleich ist und nicht die Nutzlast mitvariiert.
        if self.query_class not in SEARCH_CLASSES:
            hit = True
        keyword = self.needle if hit else MISS_KEYWORD
        parts = ["load", str(index), keyword]
        if token is not None:
            # Der Subscriber schneidet ab "__probe__:" bis zum naechsten
            # Whitespace. Der Token muss deshalb am Ende stehen.
            parts.append(token)
        return " ".join(parts)

    def row_for(
        self, index: int, *, token: str | None = None, hit: bool = True
    ) -> tuple[int, int, str, int]:
        """(topic_id, user_id, content, match_value) fuer einen INSERT."""
        return (
            self.topic_for(index),
            self.user_for(index),
            self.content_for(index, token=token, hit=hit),
            self.match_value_for(index),
        )


INSERT_SQL = (
    "INSERT INTO messages (topic_id, user_id, content, created_at, match_value) "
    "VALUES (%s, %s, %s, now(), %s)"
)


def insert_rows(conn, rows: Iterable[tuple[int, int, str, int]]) -> None:
    rows = list(rows)
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
    conn.commit()


def max_message_id(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT coalesce(max(id), 0) FROM messages")
        return int(cur.fetchone()[0])


def delete_above(conn, baseline_id: int) -> int:
    """Entfernt alle waehrend des Laufs eingefuegten Nachrichten."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM messages WHERE id > %s", (baseline_id,))
        removed = cur.rowcount
    conn.commit()
    return int(removed)


def dataset_shape(conn) -> dict[str, int]:
    """Liefert die Groesse des vorhandenen Datenbestands."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT (SELECT count(*) FROM topics), "
            "       (SELECT count(*) FROM users), "
            "       (SELECT count(*) FROM messages)"
        )
        topics, users, messages = cur.fetchone()
    return {"topics": int(topics), "users": int(users), "messages": int(messages)}


def assert_dataset_matches(conn, *, topics: int, users: int) -> dict[str, int]:
    """
    Bricht ab, wenn die Parameter nicht zum geseedeten Bestand passen.

    Die Pruefung verhindert Writes auf nicht vorhandene Topic-IDs.
    """
    shape = dataset_shape(conn)
    problems = []
    if shape["topics"] != topics:
        problems.append(f"--topics {topics}, in der DB stehen {shape['topics']}")
    if shape["users"] != users:
        problems.append(f"--users {users}, in der DB stehen {shape['users']}")
    if problems:
        raise SystemExit(
            "Parameter passen nicht zum geseedeten Bestand: "
            + "; ".join(problems)
        )
    return shape


class LoadWriter(threading.Thread):
    """
    Hintergrundschreiblast mit fester Rate.

    Jede Zeile adressiert per Round Robin genau einen Query-Index. `hit_ratio`
    steuert, welcher Anteil davon die ILIKE-Textbedingung erfuellt; damit laesst
    sich die Trefferrate stufenweise fahren, ohne den Bestand zu tauschen.
    """

    def __init__(
        self,
        *,
        connect: Callable[[], Any],
        spec: WorkloadSpec,
        rate_per_s: float,
        hit_ratio: float = 1.0,
        tick_s: float = 0.05,
        seed: int = 1234,
    ) -> None:
        super().__init__(daemon=True)
        self.connect = connect
        self.spec = spec
        self.rate_per_s = rate_per_s
        self.hit_ratio = hit_ratio
        self.tick_s = tick_s
        self._stop = threading.Event()
        self._rng = random.Random(seed)
        self.written = 0
        self.errors: list[str] = []
        self._started_at: float | None = None
        self._stopped_at: float | None = None

    def stop(self) -> None:
        self._stop.set()

    @property
    def elapsed_s(self) -> float:
        """
        Eigene Laufzeit des Writers.

        Der Writer startet vor der Beruhigungszeit und laeuft waehrend der
        gesamten Messung. Die eigene Laufzeit ist daher die Bezugsdauer fuer
        die erreichte Rate.
        """
        if self._started_at is None:
            return 0.0
        end = self._stopped_at if self._stopped_at is not None else time.monotonic()
        return max(end - self._started_at, 0.0)

    @property
    def achieved_rate(self) -> float:
        el = self.elapsed_s
        return self.written / el if el > 0 else 0.0

    def run(self) -> None:
        if self.rate_per_s <= 0:
            return
        conn = self.connect()
        conn.autocommit = False
        self._started_at = time.monotonic()
        index = 0
        per_tick = self.rate_per_s * self.tick_s
        carry = 0.0
        next_at = time.monotonic()

        try:
            while not self._stop.is_set():
                carry += per_tick
                n = int(carry)
                carry -= n

                if n > 0:
                    rows = []
                    for _ in range(n):
                        hit = self._rng.random() < self.hit_ratio
                        rows.append(self.spec.row_for(index, hit=hit))
                        index = (index + 1) % self.spec.total_queries
                    try:
                        insert_rows(conn, rows)
                        self.written += len(rows)
                    except Exception as exc:  # noqa: BLE001
                        conn.rollback()
                        self.errors.append(str(exc))

                next_at += self.tick_s
                sleep = next_at - time.monotonic()
                if sleep > 0:
                    time.sleep(sleep)
                else:
                    # Rate nicht haltbar; Takt neu setzen statt aufzuholen.
                    next_at = time.monotonic()
        finally:
            self._stopped_at = time.monotonic()
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
