-- scripts/repartition_orders.sql
-- Purpose: Repartition raw TPCH orders data into a Hive-style partitioned layout
--          Uses DuckLake to create a partitioned table and copy data from orders_raw
-- Usage:   make repartition
--
-- This script reads from the orders_raw table (populated via bootstrap_catalog.sql)
-- and writes to a partitioned orders table organized by year/month.

-- ============================================================================
-- Initialize DuckLake Extension
-- ============================================================================
INSTALL ducklake;
LOAD ducklake;

-- ============================================================================
-- Attach DuckLake Catalog Database
-- ============================================================================
-- The catalog database stores all metadata; DATA_PATH specifies where Parquet files reside
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- ============================================================================
-- Create Partitioned Table Schema
-- ============================================================================
-- Schema matches TPCH specification with additional partition columns (year, month, day)
CREATE OR REPLACE TABLE orders (
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderstatus VARCHAR,
    o_totalprice DECIMAL(15,2),
    o_orderdate DATE,
    o_orderpriority VARCHAR,
    o_clerk VARCHAR,
    o_shippriority INTEGER,
    o_comment VARCHAR,
    year INTEGER,
    month INTEGER,
    day INTEGER
);

-- ============================================================================
-- Configure Partitioning
-- ============================================================================
-- Partition by year and month for Hive-style directory layout: year=YYYY/month=MM/
ALTER TABLE orders SET PARTITIONED BY (year, month);

-- ============================================================================
-- Load Data with Partitioning
-- ============================================================================
-- Extract date components and copy from orders_raw into partitioned orders table
-- ORDER BY ensures data is written in chronological order
INSERT INTO orders
SELECT
    *,
    year(o_orderdate)  AS year,
    month(o_orderdate) AS month,
    day(o_orderdate)   AS day
FROM orders_raw
ORDER BY o_orderdate;

