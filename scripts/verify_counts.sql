-- scripts/verify_counts.sql
-- Purpose: Verify row counts match between raw and partitioned tables
-- Usage:   make verify
--
-- Compares row counts to ensure data integrity after repartitioning.
-- Both tables should have identical row counts if repartitioning succeeded.

-- ============================================================================
-- Initialize DuckLake Extension
-- ============================================================================
INSTALL ducklake;
LOAD ducklake;

-- ============================================================================
-- Attach DuckLake Catalog Database
-- ============================================================================
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- ============================================================================
-- Verify Row Counts
-- ============================================================================
-- Counts in raw pq files
SELECT 'tpch_orders' AS name, COUNT(*) AS rows
FROM read_parquet('data/tpch/orders/*.parquet');

-- Count rows in raw table (zero-copy registered files)
SELECT 'raw_orders' AS name, COUNT(*) AS rows
FROM lake.orders_raw;

-- Count rows in partitioned table (loaded via repartition step)
SELECT 'lake_orders' AS name, COUNT(*) AS rows
FROM lake.orders;

-- Counts in raw lineitem pq files
SELECT 'tpch_lineitem' AS name, COUNT(*) AS rows
FROM read_parquet('data/tpch/lineitem/*.parquet');

-- Count rows in partitioned lineitem table (if it exists)
SELECT 'lake_lineitem' AS name, COUNT(*) AS rows
FROM lake.lineitem;

