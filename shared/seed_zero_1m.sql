\set ON_ERROR_STOP on

-- Aufruf:
-- psql -v messages=1000000 -v topics=60000 -v users=1000 -f seed_zero_1m.sql
--
-- Zwei Kontrollmechanismen stecken im Seed:
--
-- 1. match_value = id * 1000 erlaubt eine Range-Query [lo, lo+1), die
--    initial genau eine Zeile trifft (Selektivitaetskontrolle nach
--    Wingerath). Query-Index k adressiert Zeile id = k+1.
--
-- 2. Jede Zeile bekommt ein Keyword im content. Die Verteilung haengt am
--    Index INNERHALB des Topics, nicht an g selbst, weil g innerhalb eines
--    Topics in Schritten von :topics laeuft und jede lineare Funktion von g
--    dann konstant bliebe. Das Muster wiederholt sich alle zehn Zeilen eines
--    Topics:
--
--      alpha  1 von 10
--      beta   2 von 10
--      gamma  5 von 10
--      delta  2 von 10
--
--    Die tatsaechlichen Anteile treffen 10/20/50/20 nur dann exakt,
--    wenn :messages / :topics ein Vielfaches von zehn ist. Sonst faellt der
--    angebrochene letzte Zehnerblock jedes Topics ueberproportional auf die
--    vorderen Keywords. Beispiel 1.000.000 / 30.000 = 33,3 Zeilen pro Topic
--    ergibt gemessen rund 12/24/46/18. Das ist kein Fehler, sondern
--    Quantisierung; der Preflight druckt die gemessenen Anteile, und die
--    Der Preflight gibt die tatsaechlichen Anteile aus. Fuer exakte nominelle
--    Anteile muss :topics so gewaehlt werden, dass
--    :messages / :topics durch zehn teilbar ist, etwa 25.000 Topics bei
--    1.000.000 Nachrichten (40 Zeilen pro Topic, exakt 10/20/50/20).
--
--    Bewusst keine Hashfunktion: die wuerde global exakt treffen, aber die
--    Trefferrate pro Topic zufaellig streuen lassen, und dann misst jede Query
--    eine andere Selektivitaet.

TRUNCATE TABLE messages, topics, users RESTART IDENTITY CASCADE;

DROP INDEX IF EXISTS idx_messages_topic_created_id;
DROP INDEX IF EXISTS idx_messages_match_value;
DROP INDEX IF EXISTS idx_messages_topic_user;

INSERT INTO topics (id, name)
SELECT g, 'Topic ' || g
FROM generate_series(1, :topics) AS g;

INSERT INTO users (id, name)
SELECT g, 'User ' || g
FROM generate_series(1, :users) AS g;

INSERT INTO messages (id, topic_id, user_id, content, created_at, match_value)
SELECT
    g,
    ((g - 1) % :topics) + 1,
    ((g - 1) % :users) + 1,
    'seed message ' || g || ' '
        || CASE mod(div(g - 1, :topics::bigint), 10)
               WHEN 0 THEN 'alpha'
               WHEN 1 THEN 'beta'
               WHEN 2 THEN 'beta'
               WHEN 8 THEN 'delta'
               WHEN 9 THEN 'delta'
               ELSE 'gamma'
           END,
    TIMESTAMPTZ '2026-01-01 00:00:00+00' + (g * INTERVAL '1 millisecond'),
    g * 1000
FROM generate_series(1, :messages) AS g;

SELECT setval(
    pg_get_serial_sequence('messages', 'id'),
    (SELECT max(id) FROM messages),
    true
);

CREATE INDEX idx_messages_topic_created_id
    ON messages (topic_id, created_at DESC, id DESC);

CREATE INDEX idx_messages_match_value
    ON messages (match_value);

CREATE INDEX idx_messages_topic_user
    ON messages (topic_id, user_id);

ANALYZE topics;
ANALYZE users;
ANALYZE messages;

SELECT
    (SELECT count(*) FROM topics)   AS topics,
    (SELECT count(*) FROM users)    AS users,
    (SELECT count(*) FROM messages) AS messages,
    (SELECT round(count(*)::numeric / NULLIF((SELECT count(*) FROM topics), 0), 1)
       FROM messages)               AS messages_per_topic;

SELECT
    (SELECT count(*) FROM messages WHERE content LIKE '%alpha%') AS alpha,
    (SELECT count(*) FROM messages WHERE content LIKE '%beta%')  AS beta,
    (SELECT count(*) FROM messages WHERE content LIKE '%gamma%') AS gamma,
    (SELECT count(*) FROM messages WHERE content LIKE '%delta%') AS delta;
