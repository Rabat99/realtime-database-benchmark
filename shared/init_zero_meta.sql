-- Laeuft einmalig beim ersten Start der Meta-Instanz.
-- Zero braucht CVR- und Change-DB getrennt vom Upstream, sonst laeuft die
-- selbst erzeugte Last mit wachsender Query-Anzahl auf derselben Instanz wie
-- der Workload und verfaelscht die Skalierungsachse.

CREATE DATABASE zero_change;
