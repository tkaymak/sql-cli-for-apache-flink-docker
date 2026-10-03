SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

-- Beispiel 03: Fenster-Aggregationen (TUMBLE, HOP, SESSION) mit Window Table-Valued Functions (TVFs)
-- Voraussetzung: Beispiel 02 läuft und befüllt das Kafka-Topic 'orders'.
-- Idle-Timeout setzen, damit Watermarks auch bei temporär inaktiven Kafka-Partitionen fortschreiten.
SET 'table.exec.source.idle-timeout' = '5 s';

-- Kafka-Tabelle als Datenquelle mit Event-Time und Watermark definieren.
CREATE TEMPORARY TABLE orders_kafka (
  order_id STRING, customer STRING, product STRING, amount DOUBLE, order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-playground',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Begrenzter Snapshot für Window TVFs:
-- Dynamic Table Hints (/*+ OPTIONS(...) */) sind innerhalb von Window-TVF-Argumenten ('TABLE ...') nicht erlaubt (Parse-Error).
-- Ein unbegrenztes Lesen von orders_kafka würde zudem nie terminieren (run-example.sh bliebe hängen).
-- 'scan.bounded.mode' = 'latest-offset' liest von Beginn bis zum aktuellen Offset; bei Input-Ende
-- emittiert Flink ein finales Watermark, alle Fenster feuern und der Job beendet sich sauber.
-- Die unbegrenzte Variante (direkt auf orders_kafka) läuft im interaktiven Client endlos weiter (Abbruch mit Strg+C).
CREATE TEMPORARY TABLE orders_snapshot WITH ('scan.bounded.mode' = 'latest-offset') LIKE orders_kafka;

-- 1. Tumbling Window: nicht überlappendes 10-Sekunden-Fenster aggregiert Bestellanzahl und Umsatz pro Produkt.
SELECT window_start, window_end, product, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
FROM TABLE(TUMBLE(TABLE orders_snapshot, DESCRIPTOR(order_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, product
LIMIT 10;

-- 2. Hopping Window: Gleitendes 20-Sekunden-Fenster mit 5-Sekunden-Vorrücken (überlappende Fenster).
SELECT window_start, window_end, product, COUNT(*) AS orders
FROM TABLE(HOP(TABLE orders_snapshot, DESCRIPTOR(order_time), INTERVAL '5' SECOND, INTERVAL '20' SECOND))
GROUP BY window_start, window_end, product
LIMIT 10;

-- 3. Session Window: Sitzungsfenster mit 5 Sekunden Inaktivitätslücke pro Kunde.
SELECT window_start, window_end, customer, COUNT(*) AS orders
FROM TABLE(SESSION(TABLE orders_snapshot PARTITION BY customer, DESCRIPTOR(order_time), INTERVAL '5' SECOND))
GROUP BY window_start, window_end, customer
LIMIT 10;
