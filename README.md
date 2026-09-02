# Realtime-Database-Benchmark

Dieses Repository enthält die Benchmarkumgebung der Bachelorarbeit zum Vergleich der Live-Query-Mechanismen von **Hasura** und **Zero**.

Der Benchmark trennt drei Untersuchungsbereiche:

- Subscription-Initialisierung bei steigender Client- oder Queryzahl,
- Steady State bei steigender Query- oder Schreibrate,
- qualitative Prüfung der unterstützten Query-Operatoren.

Hasura und Zero werden nacheinander auf demselben PostgreSQL-Datenbestand ausgeführt. Dadurch bleiben Schema, Seed und Schreiblast zwischen den Systemen gleich.

## Verzeichnisstruktur

| Pfad | Inhalt |
| --- | --- |
| `benchmark/` | Python-Harness, Lastgenerator, Messreihen und Systemadapter |
| `compose/` | Docker-Compose-Dateien für Hasura, Zero und PostgreSQL |
| `shared/` | gemeinsames Schema und deterministischer Seed |
| `zero/` | Zero Query API und Subscriber-Client |
| `results/` | archivierte Rohdaten und daraus abgeleitete CSV-Dateien |
| `runs/` | Ausgaben neu gestarteter Messungen; wird nicht versioniert |

Die systemspezifische Subscription-Anbindung ist in `benchmark/hasura_adapter.py` und `benchmark/zero_adapter.py` gekapselt. Beide Adapter stellen den Messprogrammen dieselben grundlegenden Lebenszyklusoperationen bereit. Workload, Seed und Auswertungslogik bleiben davon getrennt.

## Referenzumgebung

Die archivierten Messungen wurden in folgender Umgebung erzeugt:

| Komponente | Version oder Zuteilung |
| --- | --- |
| Betriebssystem | Debian 13, Kernel 6.12.74 |
| VM | 16 vCPU, AMD EPYC 7542, 32 GB RAM, KVM |
| Docker | 29.5.3 |
| Docker Compose | 5.1.4 |
| PostgreSQL | 16.14 |
| Hasura | 2.42.0 |
| Zero | 1.8.0 |
| PostgreSQL-Konfiguration | `shared_buffers=1GB`, `work_mem=16MB`, `wal_level=logical` |
| Hasura Live Queries | Refetch 1000 ms, Batchgröße 100 |

Die Baseline verwendet CPU 0 für PostgreSQL, CPU 1 für das untersuchte Realtime-System und CPU 2 sowie 3 für den Benchmark-Harness. Hasura und Zero werden nicht gleichzeitig vermessen.

Die Benchmarksoftware ist nicht an einen bestimmten Benutzernamen oder Installationspfad gebunden. Vergleichbare Performancewerte setzen jedoch eine hinreichend ähnliche Hardware- und Systemkonfiguration voraus.

## Voraussetzungen

Erforderlich sind:

- Linux mit mindestens vier logischen CPUs,
- Docker Engine mit Docker Compose,
- Python 3.13 mit `venv`,
- GNU Make,
- `taskset` aus `util-linux`,
- Internetzugang beim ersten Start zum Abruf der Docker-Images und Python-Abhängigkeiten.

Der ausführende Benutzer muss Docker ohne `sudo` verwenden dürfen. Für die großen Initialisierungsmessungen wurden in der Referenzumgebung 16 logische CPUs verwendet.

PostgreSQL, Hasura und Zero müssen nicht separat installiert werden. Sie werden über Docker Compose gestartet.

## Einrichtung

Nach dem Klonen oder Entpacken des Repositorys im Wurzelverzeichnis ausführen:

```bash
cp .env.example .env
make doctor
make venv
make db-seed
```

`make doctor` prüft Docker, Docker Compose, Python, `taskset`, den Node-Lockfile sowie die konfigurierte CPU-Zuordnung.

`.env` enthält lokale Zugangsdaten und systemspezifische Einstellungen. Die Datei ist durch `.gitignore` ausgeschlossen und darf nicht committed werden. Für eine lokale Funktionsprüfung können die Beispielwerte aus `.env.example` verwendet werden; auf einem erreichbaren System sollten eigene Zugangsdaten gesetzt werden.

Die Python-Abhängigkeiten sind in `benchmark/requirements.txt` festgelegt. Der Zero-Build verwendet `zero/package-lock.json`.

### Datenbank und Seed

`make db-seed` erzeugt das gemeinsame Schema und befüllt den Datenbestand deterministisch mit:

- 1.000.000 Nachrichten,
- 30.000 Topics,
- 1.000 Benutzern.

Der Seed leert die drei Workload-Tabellen vor dem erneuten Befüllen. Er darf deshalb nicht während oder zwischen zusammengehörenden Messpunkten ausgeführt werden.

Der gemessene Datenbankstand besitzt:

- die Tabellen `messages`, `topics` und `users`,
- Primärschlüssel auf allen drei Tabellen,
- die Fremdschlüssel `messages.topic_id -> topics.id` und `messages.user_id -> users.id`,
- die Indizes `idx_messages_topic_created_id`, `idx_messages_match_value` und `idx_messages_topic_user`,
- `REPLICA IDENTITY DEFAULT`.

## Schneller Funktionstest

Mit den folgenden Befehlen lässt sich prüfen, ob beide Benchmarkpfade grundsätzlich lauffähig sind.

### Hasura

```bash
make hasura-up
make hasura-preflight
make hasura-smoke
make hasura-down
```

Der Preflight prüft Datenbestand, Schema, Hasura-Healthcheck und die GraphQL-Relation zwischen Nachrichten und Benutzern.

Der Smoke-Test verwendet standardmäßig 250 eindeutige Window-Queries, zwei Clients, 250 angebotene Writes/s, 100 Probes, 30 Sekunden Settle-Phase und 60 Sekunden Messdauer. Das Ergebnis wird unter `runs/hasura_smoke.json` gespeichert.

### Zero

```bash
make zero-up
make zero-preflight
make zero-smoke
make zero-down
```

Beim ersten Start baut Zero seine lokale Replica auf. `make zero-preflight` muss `PASS` ausgeben, bevor eine Messung gestartet wird. Geprüft werden unter anderem Replikationsslot, WAL-Rückstand, CVR-Größe, Zeilenzahlen und Erreichbarkeit des Zero-Caches.

Das Ergebnis wird unter `runs/zero_smoke.json` gespeichert.

Die `up`-Targets stoppen automatisch das jeweils andere Realtime-System. Die `down`-Targets entfernen keine Datenvolumes.

## Einzelnen Steady-State-Lauf konfigurieren

Make-Variablen können direkt beim Aufruf überschrieben werden.

Beispiel für 2.000 eindeutige Window-Join-Queries bei Hasura und 250 Writes/s:

```bash
make hasura-smoke \
  QUERY_CLASS=window_join \
  QUERIES=2000 \
  CLIENTS=2 \
  LOAD_RATE=250 \
  REGISTRATION_RATE=50
```

Für Zero beispielsweise:

```bash
make zero-smoke \
  QUERY_CLASS=window_join \
  QUERIES=1800 \
  CLIENTS=2 \
  LOAD_RATE=250
```

Die Parameter `PROBES`, `SETTLE`, `MEASURE_SECONDS`, `DRAIN_SECONDS`, `TIMEOUT`, `HARNESS_CORES` und `SEED` können ebenfalls überschrieben werden.

## Reproduktion der in der Arbeit ausgewerteten Lastpunkte

Die in der Bachelorarbeit ausgewerteten Lastpunkte sind in den aufbereiteten Datensätzen unter `results/derived/` dokumentiert. Ein einzelner Lastpunkt kann mit den oben beschriebenen `hasura-smoke`- beziehungsweise `zero-smoke`-Targets erneut ausgeführt werden, indem insbesondere `QUERY_CLASS`, `QUERIES` und `LOAD_RATE` auf den gewünschten Messpunkt gesetzt werden.

Für die Query-Skalierung wird entsprechend der Arbeit die Schreibrate auf `LOAD_RATE=250` fixiert und die Queryzahl variiert. Für die Write-Skalierung von Zero wird `QUERIES=250` fixiert und die Schreibrate variiert. Eine separate Write-Skalierungsreihe für Hasura wurde in der Arbeit nicht durchgeführt.

Die zusätzlich im Repository vorhandenen Series-Targets automatisieren mehrere Einzelläufe. Sie sind für die Reproduktion einzelner in der Arbeit beschriebener Messpunkte nicht erforderlich und werden hier nicht als Bestandteil der in Kapitel 4 beschriebenen Versuchsmethodik verwendet.

## Subscription-Initialisierung

Die parallele Initialisierung verwendet einen phasengleichen Start über eine Barriere. `INIT_SUBSCRIBERS` bezeichnet die Zahl der Queries und `INIT_CLIENTS` die Zahl physischer Clients.

Beispiel für 100 parallele Window-Subscriptions:

```bash
make hasura-init \
  QUERY_CLASS=window \
  INIT_SUBSCRIBERS=100 \
  INIT_CLIENTS=100

make zero-init \
  QUERY_CLASS=window \
  INIT_SUBSCRIBERS=100 \
  INIT_CLIENTS=100
```

Bei der Dichtemessung werden viele Queries auf 100 physische Clients verteilt:

```bash
make hasura-density-init \
  QUERY_CLASS=window_join \
  DENSITY_SUBSCRIBERS=5000 \
  DENSITY_CLIENTS=100

make zero-density-init \
  QUERY_CLASS=window_join \
  DENSITY_SUBSCRIBERS=5000 \
  DENSITY_CLIENTS=100
```

Für die großen Zero-Initialisierungsmessungen wurden die Subscriber-Prozesse auf CPU 2 bis 13 und der Python-Koordinator auf CPU 14 und 15 ausgeführt. Dazu vor dem Zero-Start in `.env` setzen:

```dotenv
HARNESS_CPUS=2-13
COORDINATOR_CPUS=14,15
```

Für Hasura laufen die WebSocket-Clients direkt im Python-Harness. Für die entsprechende Initialisierungsreihe wurde verwendet:

```dotenv
HARNESS_CPUS=2-13
COORDINATOR_CPUS=2-13
```

Für die Baseline-Steady-State-Messungen anschließend wieder:

```dotenv
HARNESS_CPUS=2,3
COORDINATOR_CPUS=2,3
```

## Expressivität

```bash
make hasura-up
make hasura-expressivity
make hasura-down

make zero-up
make zero-preflight
make zero-expressivity
make zero-down
```

Geprüft werden AND, OR, Sortierung, Limit, Offset, Join, Count, Max und ein ILIKE-Substringfilter. Die Ausgabe unterscheidet zwischen formulierbaren, nicht formulierbaren und zur Laufzeit fehlgeschlagenen Query-Klassen.

## CPU-Profile und vertikale Skalierung

Eine Änderung der CPU-Zuordnung in `.env` wirkt nur auf neu erstellte Container. Nach einem Profilwechsel müssen die betroffenen Container daher neu erstellt werden.

Die folgenden Profile entsprechen der Baseline und den in Abschnitt 5.3 der Bachelorarbeit beschriebenen Scale-up-Kontrollversuchen:

| Profil | `PG_CPUS` | `HASURA_CPUS` | `ZERO_CPUS` | Harness |
| --- | --- | --- | --- | --- |
| Baseline | `0` | `1` | `1` | `2,3` |
| Zero mit zwei CPUs | `0` | `1` | `1,4` | `2,3` |
| PostgreSQL mit zwei CPUs | `0,4` | `1` | `1` | `2,3` |
| PostgreSQL und Hasura mit je zwei CPUs | `0,4` | `1,5` | `1` | `2,3` |

Nach dem Bearbeiten von `.env` beispielsweise für Hasura:

```bash
docker compose --env-file .env -f compose/hasura.yml \
  up -d --force-recreate postgres hasura

docker inspect compose-postgres-1 compose-hasura-1 \
  --format '{{.Name}} cpuset={{.HostConfig.CpusetCpus}}'
```

Für Zero entsprechend:

```bash
docker compose --env-file .env -f compose/zero.yml \
  up -d --force-recreate postgres postgres-meta zero-query-api zero-cache

docker inspect compose-postgres-1 compose-zero-cache-1 \
  --format '{{.Name}} cpuset={{.HostConfig.CpusetCpus}}'
```

Die ausgegebenen Cpuset-Werte sind vor dem Preflight zu kontrollieren.

## Ergebnisfelder und Gültigkeit

Die primäre Steady-State-Latenz reicht vom Beginn des Schreibvorgangs bis zur Beobachtung am Subscriber. Neue Läufe speichern sie in:

- `p50_ms` und `p95_ms`,
- `p50_e2e_ms` und `p95_e2e_ms`,
- `write_to_observation_ms` innerhalb der Probe-Samples.

Die zusätzliche Sicht ab Commit-Bestätigung steht in `p50_commit_ack_ms` und `p95_commit_ack_ms`.

Einige archivierte Rohdateien entstanden vor der Vereinheitlichung der Kurzfelder. Für den Vergleich mit den CSV-Dateien sind dort die expliziten Felder `p50_e2e_ms` und `p95_e2e_ms` maßgeblich.

Ein Steady-State-Lauf ist apparativ gültig, wenn alle Einträge unter `valid` den Wert `true` besitzen. Zusätzlich sind `delivery_ratio`, `achieved_load_rate`, Ressourcenwerte und Fehlerlisten zu prüfen. Ungültige Grenzpunkte können zur Dokumentation erhalten bleiben, werden jedoch nicht als gültige Kapazitätspunkte interpretiert.

Bei Zero werden zusätzlich die CPU-Auslastung des Zero-Serverpfads sowie der während der Messphase beobachtete Rückstand der logischen PostgreSQL-Replikation erfasst.

## Archivierte Ergebnisse

Die vorhandenen JSON-Dateien unter `results/` sind Rohdaten aus den durchgeführten Messläufen. Zusätzliche Wiederholungs-, Recheck- und Grenzläufe bleiben als Rohdaten erhalten. Sie dokumentieren unter anderem Lauf-zu-Lauf-Variation und die Eingrenzung von Lastgrenzen, sind jedoch nicht automatisch Bestandteil der für die Abbildungen aufbereiteten Datensätze.

Die sieben für die Abbildungen aufbereiteten Datensätze liegen unter `results/derived/`. Neue Ausführungen schreiben ausschließlich nach `runs/` und überschreiben damit keine archivierten Ergebnisse.

Weitere Hinweise zur Ergebnisablage stehen in `results/README.md`.
