-- scripts/make_manifest.sql
-- Purpose: Demonstrate DuckLake metadata capabilities and create snapshots
-- Usage:   make manifest
--
-- Shows how DuckLake stores all metadata in its catalog database (no external
-- manifest files needed). Demonstrates querying metadata tables directly.

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
-- Show Table Row Counts
-- ============================================================================
SELECT '=== Current Table & Row Counts ===' AS info;
SELECT 'orders_raw' AS table_name, COUNT(*) AS row_count
FROM lake.orders_raw
UNION ALL
SELECT 'orders' AS table_name, COUNT(*) AS row_count
FROM lake.orders
UNION ALL
SELECT 'lineitem' AS table_name, COUNT(*) AS row_count
FROM lake.lineitem;

-- ============================================================================
-- List Available Metadata Tables
-- ============================================================================
-- DuckLake stores metadata in __ducklake_metadata_* schemas
-- These tables provide direct access to catalog information without external files
SELECT '=== Available Metadata Tables ===' AS info;
SELECT table_name
FROM information_schema.tables
WHERE table_schema LIKE '%ducklake%' OR table_name LIKE '%ducklake%'
ORDER BY table_name;

-- ============================================================================
-- Query Data Files Metadata
-- ============================================================================
-- Shows which Parquet files are registered for each table
SELECT '=== Data Files ===' AS info;
SELECT
    t.table_name,
    df.path,
    df.record_count
FROM __ducklake_metadata_lake.ducklake_data_file df
JOIN __ducklake_metadata_lake.ducklake_table t ON df.table_id = t.table_id
ORDER BY t.table_name, df.path
LIMIT 10;

-- ============================================================================
-- Query Snapshots (Time-Travel Metadata)
-- ============================================================================
-- Snapshots enable time-travel queries: SELECT * FROM orders AT (VERSION => 1)
SELECT '=== Snapshots ===' AS info;
SELECT
    snapshot_id,
    snapshot_time
FROM __ducklake_metadata_lake.ducklake_snapshot
ORDER BY snapshot_id DESC
LIMIT 10;

-- Note: All metadata is stored in DuckLake catalog - query directly for file lists,
-- snapshots, etc. See DuckLake documentation for snapshot creation syntax.
