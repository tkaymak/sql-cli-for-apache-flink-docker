SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.runtime-mode' = 'streaming';

-- Beispiel 04: Streaming von Kafka nach PostgreSQL via JDBC
-- Voraussetzung: Container mit '--profile postgres' gestartet und Beispiel 02 läuft.
-- 1. Kafka-Eingangstabelle (Topic 'orders') mit Event-Time-Watermark definieren.
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

-- 2. PostgreSQL-Zieltabelle (product_revenue) über JDBC-Connector definieren.
CREATE TEMPORARY TABLE product_revenue (
  window_start TIMESTAMP(3), window_end TIMESTAMP(3), product STRING, orders BIGINT, revenue DOUBLE,
  PRIMARY KEY (window_start, product) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/playground',
  'table-name' = 'product_revenue',
  'username' = 'flink',
  'password' = 'flink'
);

-- 3. Streaming-Aggregation über 1-Minuten-Tumble-Fenster berechnen und kontinuierlich in PostgreSQL schreiben (Upsert via Primary Key).
INSERT INTO product_revenue
SELECT window_start, window_end, product, COUNT(*) AS orders, SUM(amount) AS revenue
FROM TABLE(TUMBLE(TABLE orders_kafka, DESCRIPTOR(order_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, product;
