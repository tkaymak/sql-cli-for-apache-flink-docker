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

-- Gibt die ersten 10 generierten Datensätze im Tableau-Modus aus und beendet danach die Abfrage.
SELECT * FROM orders_faker LIMIT 10;
