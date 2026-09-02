# Archivierte Messergebnisse

Dieses Verzeichnis enthaelt die bei der Entwicklung und Ausfuehrung des
Benchmarks erzeugten JSON-Rohdaten. Die Dateien werden nicht von neuen
Messungen ueberschrieben; neue Ausgaben landen standardmaessig unter `runs/`.

## Aufbereitete Datensaetze

`derived/` enthaelt die sieben CSV-Dateien, aus denen die Ergebnisabbildungen
erstellt wurden:

| Datei | Inhalt |
| --- | --- |
| `hasura_steady_query_scale.csv` | Hasura Queryskalierung fuer Window und Window-Join |
| `zero_steady_query_scale.csv` | Zero Queryskalierung fuer Window und Window-Join |
| `zero_steady_write_scale_window.csv` | Zero Schreibratenskalierung bei 250 Queries |
| `subscription_init_window.csv` | parallele Window-Initialisierung |
| `subscription_init_window_join.csv` | parallele Window-Join-Initialisierung |
| `subscription_init_density_window.csv` | Window-Querydichte bei 100 Clients |
| `subscription_init_density_window_join.csv` | Window-Join-Querydichte bei 100 Clients |

`nan` kennzeichnet einen nicht vorhandenen beziehungsweise nicht erfolgreich
abgeschlossenen Messpunkt. In der Schreibratenskalierung kennzeichnet
`valid_rate=0`, dass der Lastgenerator weniger als 95 Prozent der angebotenen
Rate aufrechterhalten konnte. Diese Punkte dokumentieren die Ueberlastgrenze,
werden aber nicht als gueltige Kapazitaetspunkte interpretiert.

## Rohdaten

Die JSON-Dateien im Wurzelverzeichnis enthalten neben den Latenzen auch
Parameter, Delivery Ratio, erreichte Schreibrate, Ressourcenmessungen,
Probe-Samples und Gueltigkeitsbedingungen. Dateinamen wie `repeat`, `rerun`,
`recheck`, `fill` oder `reset` stammen aus dem Versuchsablauf. Fuer die
fachliche Einordnung sind die im JSON gespeicherten Parameter massgeblich.

Wiederholungen und ungueltige Grenzpunkte bleiben erhalten, weil sie die
Streuung beziehungsweise den Abbruch einer Lastreihe dokumentieren. Sie sind
nicht automatisch Bestandteil der aufbereiteten CSV-Dateien.

Einige archivierte Dateien verwenden in `p50_ms` und `p95_ms` noch die
Commit-Ack-Latenz. Die fuer die Abbildungen verwendete Ende-zu-Ende-Latenz
steht unabhaengig davon eindeutig in `p50_e2e_ms` und `p95_e2e_ms`.

## Ausgeschlossene Dateien

Folgende nachweislich redundante oder diagnostische Dateien wurden aus der
Abgabefassung entfernt:

- `hasura_batch_init_c100_n15000_repeat.json` und
  `hasura_batch_init_c100_n17500_repeat.json`: bytegleich mit dem
  20.000er-Lauf und intern ebenfalls mit 20.000 Subscriptions beschriftet
- `zero_init_harness12_n225_repeat.json`: bytegleich mit
  `zero_init_harness12_n225.json`
- `zero_window_q50_c2_r1000_repeat2.json`: bytegleich mit dem gleichnamigen
  Basislauf
- `expr_zero_window_debug.json` und `expr_zero_check.json`: diagnostische
  Zwischenlaeufe; die zusammenfassende Auswertung liegt in `expr_zero.json`
