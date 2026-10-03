SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

-- Beispiel 05: Avro-Serialisierung mit Confluent Schema Registry
-- Voraussetzung: Container mit '--profile avro' gestartet.
-- 1. Faker-Tabelle zur Erzeugung synthetischer Bestelldaten definieren.
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

-- 2. Kafka-Tabelle mit Confluent Avro Format und Schema-Registry-Anbindung definieren.
CREATE TEMPORARY TABLE orders_avro (
  order_id STRING, customer STRING, product STRING, amount DOUBLE, order_time TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders_avro',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql-playground-avro',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://schema-registry:8085'
);

-- 3. Streaming-Job: Schreibt Daten im Avro-Format nach Kafka und registriert das Schema automatisch in der Schema Registry.
INSERT INTO orders_avro SELECT order_id, customer, product, amount, order_time FROM orders_faker;

-- 4. Verifikation: Liest 5 Avro-Datensätze aus dem Kafka-Topic aus und gibt sie im Tableau-Modus aus.
SELECT * FROM orders_avro LIMIT 5;
