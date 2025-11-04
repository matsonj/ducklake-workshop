-- scripts/verify_counts.sql
-- Purpose: Verify row counts match between raw and partitioned tables
-- Usage:   make verify; duckdb -f scripts/03_verify_counts.sql
--
-- Compares row counts to ensure data integrity after repartitioning.
-- Both tables should have identical row counts if repartitioning succeeded.

-- ============================================================================
-- Initialize DuckLake Extension
-- ============================================================================
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- ============================================================================
-- Verify Row Counts
-- ============================================================================
-- Counts in raw pq files
SELECT 'tpch_orders' AS name, COUNT(*) AS rows
FROM read_parquet('data/tpch/orders/*.parquet')
union all
SELECT 'raw_orders' AS name, COUNT(*) AS rows
FROM lake.orders_raw
union all
SELECT 'lake_orders' AS name, COUNT(*) AS rows
FROM lake.orders
union all
SELECT 'tpch_lineitem' AS name, COUNT(*) AS rows
FROM read_parquet('data/tpch/lineitem/*.parquet')
union all
SELECT 'lake_lineitem' AS name, COUNT(*) AS rows
FROM lake.lineitem;
