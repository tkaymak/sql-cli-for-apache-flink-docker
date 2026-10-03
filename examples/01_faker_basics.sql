SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

-- Beispiel 01: Flink-Faker Grundlagen
-- Erzeugt kontinuierlich synthetische Bestelldaten direkt in Flink,
-- ohne dass eine externe Datenquelle (wie Kafka oder eine Datenbank) benötigt wird.
CREATE TEMPORARY TABLE orders_faker (
  order_id   STRING,
  customer   STRING,
  product    STRING,
  amount     DOUBLE,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '5',
  'fields.order_id.expression'   = '#{Internet.uuid}',
  'fields.customer.expression'   = '#{Name.firstName}',
  'fields.product.expression'    = '#{Options.option ''Laptop'',''Phone'',''Tablet'',''Monitor'',''Keyboard''}',
  'fields.amount.expression'     = '#{Number.randomDouble ''2'',''5'',''500''}',
  'fields.order_time.expression' = '#{date.past ''5'',''SECONDS''}'
);

-- Begrenztes Lesen via Dynamic Table Option 'number-of-rows' = '10':
-- Ein unbegrenztes 'SELECT ... LIMIT 10' beendet den Streaming-Job nicht (run-example.sh würde hängen).
-- Durch den Hint stoppt Faker nach 10 Zeilen und der SQL-Client beendet sich sauber.
-- Die unbegrenzte Variante läuft im interaktiven Client endlos weiter und wird mit Strg+C gestoppt.
SELECT * FROM orders_faker /*+ OPTIONS('number-of-rows' = '10') */;
