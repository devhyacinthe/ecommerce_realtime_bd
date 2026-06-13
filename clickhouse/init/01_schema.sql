CREATE DATABASE IF NOT EXISTS ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce.sales_by_minute (
    window_start DateTime,
    window_end   DateTime,
    category     LowCardinality(String),
    country      LowCardinality(String),
    orders       UInt64,
    revenue      Float64,
    units        UInt64
) ENGINE = MergeTree()
ORDER BY (window_start, category, country);