-- scripts/change_feed.sql
-- Purpose: Show row-level changes (insertions/deletions) between snapshots
-- Usage:   make change-feed
--
-- This script demonstrates:
-- 1. Comparing two snapshots to identify changes
-- 2. Showing insertions (rows in TO but not in FROM)
-- 3. Showing deletions (rows in FROM but not in TO)
-- 4. Validating that changes reconstruct current state

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
-- Show Available Snapshots
-- ============================================================================
SELECT '=== Available Snapshots ===' AS info;
SELECT
    snapshot_id,
    snapshot_time,
    schema_version
FROM __ducklake_metadata_lake.ducklake_snapshot
ORDER BY snapshot_id DESC
LIMIT 10;

-- ============================================================================
-- Configuration (use DuckDB variables)
-- ============================================================================
-- Set defaults if not already set
-- Override via: duckdb -c "SET VARIABLE table_name = 'orders'; SET VARIABLE from_version = 5; SET VARIABLE to_version = 6;" -f scripts/change_feed.sql
SET VARIABLE table_name = 'orders';

-- Auto-detect snapshot versions if not set
-- Use latest two snapshots if variables not provided
CREATE TEMP TABLE snapshot_ids AS
SELECT 
    COALESCE(getvariable('from_version'), (SELECT snapshot_id FROM __ducklake_metadata_lake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1 OFFSET 1)) AS from_version,
    COALESCE(getvariable('to_version'), (SELECT snapshot_id FROM __ducklake_metadata_lake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1)) AS to_version;

-- Extract version values into variables for use in AT clause
SET VARIABLE from_version = (SELECT from_version FROM snapshot_ids);
SET VARIABLE to_version = (SELECT to_version FROM snapshot_ids);

-- Determine versions to compare
SELECT '=== Comparing Snapshots ===' AS info;
SELECT
    'FROM version' AS comparison_type,
    CAST(getvariable('from_version') AS VARCHAR) AS snapshot_id,
    s.snapshot_time,
    s.schema_version
FROM __ducklake_metadata_lake.ducklake_snapshot s
WHERE s.snapshot_id = getvariable('from_version')
UNION ALL
SELECT
    'TO version' AS comparison_type,
    CAST(getvariable('to_version') AS VARCHAR) AS snapshot_id,
    s.snapshot_time,
    s.schema_version
FROM __ducklake_metadata_lake.ducklake_snapshot s
WHERE s.snapshot_id = getvariable('to_version');

-- ============================================================================
-- Create temporary tables with snapshot data
-- Note: Table name is set via variable, but DuckDB requires explicit table names in FROM
-- For now, we use 'orders' as default. To use a different table, modify this SQL file
CREATE TEMP TABLE from_snapshot_data AS
SELECT * FROM lake.orders AT (VERSION => getvariable('from_version'));

CREATE TEMP TABLE to_snapshot_data AS
SELECT * FROM lake.orders AT (VERSION => getvariable('to_version'));

-- ============================================================================
-- Show Insertions (New Rows in TO Version)
-- ============================================================================
SELECT '=== Row Insertions (New in Latest Snapshot) ===' AS info;
SELECT to_data.*
FROM to_snapshot_data to_data
WHERE to_data.o_orderkey NOT IN (
    SELECT o_orderkey FROM from_snapshot_data
)
ORDER BY to_data.o_orderkey
LIMIT 100;

-- ============================================================================
-- Show Insertion Count
-- ============================================================================
SELECT '=== Insertion Summary ===' AS info;
WITH insertions AS (
    SELECT o_orderkey
    FROM to_snapshot_data
    WHERE o_orderkey NOT IN (
        SELECT o_orderkey FROM from_snapshot_data
    )
)
SELECT
    COUNT(*) AS insertion_count,
    COUNT(DISTINCT o_orderkey) AS distinct_order_keys
FROM insertions;

-- ============================================================================
-- Show Deletions (Removed Rows from FROM Version)
-- ============================================================================
SELECT '=== Row Deletions (Removed from Previous Snapshot) ===' AS info;
SELECT from_data.*
FROM from_snapshot_data from_data
WHERE from_data.o_orderkey NOT IN (
    SELECT o_orderkey FROM to_snapshot_data
)
ORDER BY from_data.o_orderkey
LIMIT 100;

-- ============================================================================
-- Show Deletion Count
-- ============================================================================
SELECT '=== Deletion Summary ===' AS info;
WITH deletions AS (
    SELECT o_orderkey
    FROM from_snapshot_data
    WHERE o_orderkey NOT IN (
        SELECT o_orderkey FROM to_snapshot_data
    )
)
SELECT
    COUNT(*) AS deletion_count,
    COUNT(DISTINCT o_orderkey) AS distinct_order_keys
FROM deletions;

-- ============================================================================
-- Validate Change Feed Completeness
-- ============================================================================
SELECT '=== Change Feed Validation ===' AS info;
WITH from_state AS (
    SELECT COUNT(*) AS count
    FROM from_snapshot_data
),
to_state AS (
    SELECT COUNT(*) AS count
    FROM to_snapshot_data
),
insertions AS (
    SELECT COUNT(*) AS count
    FROM (
        SELECT o_orderkey
        FROM to_snapshot_data
        WHERE o_orderkey NOT IN (
            SELECT o_orderkey FROM from_snapshot_data
        )
    )
),
deletions AS (
    SELECT COUNT(*) AS count
    FROM (
        SELECT o_orderkey
        FROM from_snapshot_data
        WHERE o_orderkey NOT IN (
            SELECT o_orderkey FROM to_snapshot_data
        )
    )
)
SELECT
    'FROM version row count' AS metric,
    CAST(f.count AS VARCHAR) AS value
FROM from_state f
UNION ALL
SELECT
    'TO version row count' AS metric,
    CAST(t.count AS VARCHAR) AS value
FROM to_state t
UNION ALL
SELECT
    'Insertions' AS metric,
    CAST(i.count AS VARCHAR) AS value
FROM insertions i
UNION ALL
SELECT
    'Deletions' AS metric,
    CAST(d.count AS VARCHAR) AS value
FROM deletions d
UNION ALL
SELECT
    'Reconstructed count' AS metric,
    CAST((f.count + i.count - d.count) AS VARCHAR) AS value
FROM from_state f, insertions i, deletions d
UNION ALL
SELECT
    'Matches TO version?' AS metric,
    CASE WHEN (t.count = f.count + i.count - d.count) THEN 'YES' ELSE 'NO' END AS value
FROM from_state f, to_state t, insertions i, deletions d;
