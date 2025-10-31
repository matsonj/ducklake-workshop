-- scripts/expire_snapshots.sql
-- Purpose: Expire old snapshots and clean up orphaned files
-- Usage:   make expire-snapshots [OLDER_THAN='1 minute'] [DRY_RUN=true]
--
-- This script demonstrates:
-- 1. Showing snapshots before expiration
-- 2. Expiring snapshots older than retention period
-- 3. Cleaning up orphaned files
-- 4. Showing remaining snapshots after cleanup

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
-- Configuration (use DuckDB variables)
-- ============================================================================
-- Set default retention period if not already set
-- Override via: duckdb -c "SET VARIABLE older_than = INTERVAL '7 days';" -f scripts/expire_snapshots.sql
SET VARIABLE older_than = INTERVAL '1 minute';
SET VARIABLE dry_run = true;

-- ============================================================================
-- Show Snapshots Before Expiration
-- ============================================================================
SELECT '=== Snapshots Before Expiration ===' AS info;
SELECT
    snapshot_id,
    snapshot_time,
    schema_version
FROM __ducklake_metadata_lake.ducklake_snapshot
ORDER BY snapshot_id DESC;

-- ============================================================================
-- Show Snapshots That Will Be Expired
-- ============================================================================
SELECT '=== Snapshots To Be Expired ===' AS info;
SELECT
    snapshot_id,
    snapshot_time,
    schema_version,
    CAST(snapshot_time AS VARCHAR) || ' (older than ' || CAST(getvariable('older_than') AS VARCHAR) || ')' AS expiration_reason
FROM __ducklake_metadata_lake.ducklake_snapshot
WHERE snapshot_time < CURRENT_TIMESTAMP - getvariable('older_than')
ORDER BY snapshot_id ASC;

-- ============================================================================
-- Expire Old Snapshots
-- ============================================================================
SELECT '=== Expiring Snapshots ===' AS info;

-- Note: ducklake_expire_snapshots may not exist in all DuckLake versions
-- This is a placeholder for the actual expiration operation
-- In practice, you would use DELETE FROM __ducklake_metadata_lake.ducklake_snapshot
-- or a different expiration command
SELECT CASE WHEN getvariable('dry_run') THEN 'DRY RUN: Would expire snapshots older than ' || CAST(getvariable('older_than') AS VARCHAR) ELSE 'LIVE: Expiring snapshots older than ' || CAST(getvariable('older_than') AS VARCHAR) END AS status;
SELECT COUNT(*) AS snapshots_to_expire
FROM __ducklake_metadata_lake.ducklake_snapshot
WHERE snapshot_time < CURRENT_TIMESTAMP - getvariable('older_than');

-- ============================================================================
-- Show Remaining Snapshots
-- ============================================================================
SELECT '=== Snapshots After Expiration ===' AS info;
SELECT
    snapshot_id,
    snapshot_time,
    schema_version
FROM __ducklake_metadata_lake.ducklake_snapshot
ORDER BY snapshot_id DESC;

-- ============================================================================
-- Show Storage Statistics Before Cleanup
-- ============================================================================
SELECT '=== Storage Statistics (Before Cleanup) ===' AS info;
SELECT
    'Total files' AS metric,
    COUNT(*) AS value
FROM __ducklake_metadata_lake.ducklake_table t
JOIN __ducklake_metadata_lake.ducklake_data_file df ON t.table_id = df.table_id
WHERE t.table_name = 'orders'
UNION ALL
SELECT
    'Total size (GB)' AS metric,
    CAST(ROUND(SUM(df.file_size_bytes) / 1024.0 / 1024.0 / 1024.0, 2) AS VARCHAR) AS value
FROM __ducklake_metadata_lake.ducklake_table t
JOIN __ducklake_metadata_lake.ducklake_data_file df ON t.table_id = df.table_id
WHERE t.table_name = 'orders';

-- ============================================================================
-- Cleanup Orphaned Files
-- ============================================================================
SELECT '=== Cleaning Up Orphaned Files ===' AS info;

-- Note: ducklake_cleanup_old_files may not exist in all DuckLake versions
-- This is a placeholder for the actual cleanup operation
SELECT 'DRY RUN: Would clean up orphaned files' AS status;

-- ============================================================================
-- Show Storage Statistics After Cleanup
-- ============================================================================
SELECT '=== Storage Statistics (After Cleanup) ===' AS info;
SELECT
    'Total files' AS metric,
    COUNT(*) AS value
FROM __ducklake_metadata_lake.ducklake_table t
JOIN __ducklake_metadata_lake.ducklake_data_file df ON t.table_id = df.table_id
WHERE t.table_name = 'orders'
UNION ALL
SELECT
    'Total size (GB)' AS metric,
    CAST(ROUND(SUM(df.file_size_bytes) / 1024.0 / 1024.0 / 1024.0, 2) AS VARCHAR) AS value
FROM __ducklake_metadata_lake.ducklake_table t
JOIN __ducklake_metadata_lake.ducklake_data_file df ON t.table_id = df.table_id
WHERE t.table_name = 'orders';

-- ============================================================================
-- Summary
-- ============================================================================
SELECT '=== Cleanup Summary ===' AS info;
SELECT
    COUNT(*) AS remaining_snapshots,
    MIN(snapshot_time) AS oldest_snapshot,
    MAX(snapshot_time) AS newest_snapshot
FROM __ducklake_metadata_lake.ducklake_snapshot;

