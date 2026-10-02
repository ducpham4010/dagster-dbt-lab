-- Runs once, on first start of an empty data volume.
-- raw:       tables ingested by Dagster Python assets (dbt sources)
-- analytics: dbt models + seeds
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.raw_customers (
    id         INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name  VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS raw.raw_orders (
    id         INTEGER PRIMARY KEY,
    user_id    INTEGER,
    order_date TIMESTAMP,
    status     VARCHAR(50)
);
