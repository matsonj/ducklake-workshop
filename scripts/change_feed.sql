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
-- Get snapshot versions (from wrapper script)
-- ============================================================================
-- Snapshot IDs are dynamically determined by the wrapper script and embedded as literals
-- __FROM_VERSION__ and __TO_VERSION__ are placeholders that get replaced by the shell script
-- __TABLE__ is replaced with the table name (defaults to lineitem)
CREATE TEMP TABLE snapshot_ids AS
SELECT 
    __FROM_VERSION__ AS from_version,
    __TO_VERSION__ AS to_version;

-- Store table name for use throughout script
CREATE TEMP TABLE config AS
SELECT '__TABLE__' AS table_name;

-- Determine versions to compare
SELECT '=== Comparing Snapshots ===' AS info;
SELECT
    'FROM version' AS comparison_type,
    CAST(from_version AS VARCHAR) AS snapshot_id,
    s.snapshot_time,
    s.schema_version
FROM snapshot_ids
JOIN __ducklake_metadata_lake.ducklake_snapshot s ON snapshot_ids.from_version = s.snapshot_id
UNION ALL
SELECT
    'TO version' AS comparison_type,
    CAST(to_version AS VARCHAR) AS snapshot_id,
    s.snapshot_time,
    s.schema_version
FROM snapshot_ids
JOIN __ducklake_metadata_lake.ducklake_snapshot s ON snapshot_ids.to_version = s.snapshot_id;

-- ============================================================================
-- Create temporary tables with snapshot data using the snapshot IDs
-- ============================================================================
-- Snapshot IDs are embedded as literals by the wrapper script
-- __FROM_VERSION__ and __TO_VERSION__ are placeholders that get replaced
-- Table name (__TABLE__) is replaced by wrapper script with actual table name
CREATE TEMP TABLE from_snapshot_data AS
SELECT * FROM __TABLE__ AT (VERSION => __FROM_VERSION__);

CREATE TEMP TABLE to_snapshot_data AS
SELECT * FROM __TABLE__ AT (VERSION => __TO_VERSION__);

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
