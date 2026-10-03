CREATE TABLE IF NOT EXISTS product_revenue (
  window_start TIMESTAMP NOT NULL,
  window_end   TIMESTAMP NOT NULL,
  product      VARCHAR(64) NOT NULL,
  orders       BIGINT,
  revenue      DOUBLE PRECISION,
  PRIMARY KEY (window_start, product)
);
