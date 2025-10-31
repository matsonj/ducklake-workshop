-- scripts/time_travel.sql
-- Purpose: Demonstrate DuckLake time travel capabilities (querying tables at specific snapshots)
-- Usage:   make time-travel; duckdb -f scripts/07_time_travel.sql
--
-- This script demonstrates:
-- 1. Listing available snapshots
-- 2. Querying a table at a specific snapshot version
-- 3. Querying a table at a specific timestamp
-- 4. Comparing current vs historical state

-- ============================================================================
-- Initialize DuckLake Extension
-- ============================================================================
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- ============================================================================
-- Show Current State
-- ============================================================================
SELECT '=== Current Table State ===' AS info;
SELECT 'orders' AS table_name, COUNT(*) AS row_count
FROM lake.orders;

-- ============================================================================
-- List Available Snapshots (Introspection)
-- ============================================================================
SELECT '=== Available Snapshots ===' AS info;
SELECT
    snapshot_id,
    snapshot_time,
    schema_version
FROM __ducklake_metadata_lake.ducklake_snapshot
ORDER BY snapshot_id DESC;

-- ============================================================================
-- Time Travel: Query at Specific Version
-- ============================================================================
-- Example: Query at snapshot version 4 (shows difference - this snapshot had 0 rows)
-- Note: Use AT (VERSION => <snapshot_id>) to query at a specific snapshot
SELECT '=== Time Travel Comparison (Current vs Snapshot 4) ===' AS info;
SELECT
    'Current' AS version_label,
    COUNT(*) AS row_count
FROM lake.orders
UNION ALL
SELECT
    'Snapshot 4' AS version_label,
    COUNT(*) AS row_count
FROM lake.orders AT (VERSION => 4);

-- ============================================================================
-- Time Travel: Query at Specific Timestamp
-- ============================================================================
-- Example: Query at the timestamp of the oldest snapshot
-- Note: To use a specific timestamp, use: AT (TIMESTAMP => '2025-10-29 22:41:30.866995-07')
SELECT '=== Time Travel Examples ===' AS info;
SELECT
    'Current' AS query_type,
    COUNT(*) AS row_count
FROM lake.orders;

-- ============================================================================
-- Detailed Snapshot Information
-- ============================================================================
SELECT '=== Snapshot Details ===' AS info;
SELECT
    snapshot_id,
    snapshot_time,
    schema_version
FROM __ducklake_metadata_lake.ducklake_snapshot
ORDER BY snapshot_id DESC
LIMIT 10;

