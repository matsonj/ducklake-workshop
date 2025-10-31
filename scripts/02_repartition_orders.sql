-- scripts/repartition_orders.sql
-- Purpose: Repartition raw TPCH orders data into a Hive-style partitioned layout
--          Uses DuckLake to create a partitioned table and copy data from orders_raw
-- Usage:   make repartition; duckdb -f scripts/02_repartition_orders.sql
--
-- This script reads from the orders_raw table (populated via bootstrap_catalog.sql)
-- and writes to a partitioned orders table organized by year/month.

-- ============================================================================
-- Initialize DuckLake Extension
-- ============================================================================
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- ============================================================================
-- Load Data with Partitioning
-- ============================================================================
-- Extract date components and copy from orders_raw into partitioned orders table
-- ORDER BY ensures data is written in chronological order
INSERT INTO lake.orders
SELECT
    *,
    year(o_orderdate)  AS year,
    month(o_orderdate) AS month,
    day(o_orderdate)   AS day
FROM lake.orders_raw;

