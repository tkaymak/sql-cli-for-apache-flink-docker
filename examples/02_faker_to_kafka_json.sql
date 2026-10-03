SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

-- Beispiel 02: Streaming von Faker nach Apache Kafka (JSON)
-- 1. Faker-Tabelle zur Erzeugung von Zufallsbestellungen definieren.
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

-- 2. Kafka-Tabelle (Topic 'orders') mit JSON-Formatierung definieren.
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

-- 3. Kontinuierlicher Streaming-Job: Schreibt Daten von Faker nach Kafka.
-- Der Job läuft nach dem Absenden im Hintergrund im Flink-Cluster weiter.
INSERT INTO orders_kafka SELECT * FROM orders_faker;
