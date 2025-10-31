-- scripts/compaction.sql
-- Purpose: Compact small files in DuckLake tables to improve query performance
-- Usage:   make compact [TABLE=orders] [TARGET_SIZE=134217728] [PARTITION_FILTER="year=1992"]
--
-- This script demonstrates:
-- 1. Showing table file statistics before compaction
-- 2. Running compaction (merging adjacent files)
-- 3. Showing improved file statistics after compaction

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
-- Set default table name if not already set
-- Override via: duckdb -c "SET VARIABLE table_name = 'orders';" -f scripts/compaction.sql
SET VARIABLE table_name = 'lineitem';

-- ============================================================================
-- Create Helper View for Table Info (ducklake_table_info compatibility)
-- ============================================================================
-- Store the current table_id BEFORE compaction to ensure we only count current files
-- Use variable if provided, otherwise default to 'lineitem'
CREATE TEMP TABLE compaction_context AS
SELECT table_id AS original_table_id
FROM __ducklake_metadata_lake.ducklake_table
WHERE table_name = getvariable('table_name')
ORDER BY table_id DESC
LIMIT 1;

CREATE OR REPLACE VIEW ducklake_table_info_view AS
SELECT
    t.table_name,
    df.path AS file_path,
    df.file_size_bytes,
    df.record_count,
    -- Extract partition path from file path if partitioned (simplified)
    CASE
        WHEN df.path LIKE '%year=%' THEN
            REGEXP_EXTRACT(df.path, 'year=[^/]+')
        ELSE NULL
    END AS partition_path
FROM __ducklake_metadata_lake.ducklake_table t
JOIN __ducklake_metadata_lake.ducklake_data_file df ON t.table_id = df.table_id
WHERE t.table_name = getvariable('table_name')
  AND t.table_id = (SELECT original_table_id FROM compaction_context);

-- ============================================================================
-- Show Table Info Before Compaction
-- ============================================================================
SELECT '=== Table File Statistics (Before Compaction) ===' AS info;
CREATE TEMP TABLE before_compaction_stats AS
SELECT 
    COUNT(*) AS file_count,
    AVG(file_size_bytes) AS avg_file_size_bytes
FROM ducklake_table_info_view
WHERE table_name = getvariable('table_name');

SELECT
    getvariable('table_name') AS table_name,
    COUNT(*) AS file_count,
    SUM(file_size_bytes) AS total_size_bytes,
    AVG(file_size_bytes) AS avg_file_size_bytes,
    MIN(file_size_bytes) AS min_file_size_bytes,
    MAX(file_size_bytes) AS max_file_size_bytes
FROM ducklake_table_info_view
WHERE table_name = getvariable('table_name');

-- ============================================================================
-- Show File Distribution by Partition (if partitioned)
-- ============================================================================
SELECT '=== File Distribution by Partition ===' AS info;
SELECT
    partition_path,
    COUNT(*) AS file_count,
    SUM(file_size_bytes) AS partition_size_bytes
FROM ducklake_table_info_view
WHERE table_name = getvariable('table_name')
GROUP BY partition_path
ORDER BY file_count DESC
LIMIT 10;

-- ============================================================================
-- Run Compaction
-- ============================================================================
SELECT '=== Running Compaction ===' AS info;

-- Use DuckLake's built-in merge_adjacent_files function to compact files
-- This merges adjacent Parquet files without expiring snapshots, preserving
-- time travel and data change feeds. See:
-- https://ducklake.select/docs/stable/duckdb/maintenance/merge_adjacent_files
-- 

-- Compaction breaks INSERTION ORDER guarantees, so we need to turn that off.
CALL lake.set_option('per_thread_output', 'false');

-- CALL ducklake_merge_adjacent_files('lake');
-- Or check your DuckLake version - this function may need different parameters
CALL ducklake_merge_adjacent_files('lake', getvariable('table_name'));

-- Restore per_thread_output to true
CALL lake.set_option('per_thread_output', 'true');

SELECT 'Compaction completed!' AS status;
SELECT 'Note: Old files are not immediately deleted. Now cleaning up old files.' AS note;

CALL ducklake_cleanup_old_files('lake',cleanup_all => true);

-- ============================================================================
-- Show Table Info After Compaction
-- ============================================================================
SELECT '=== Table File Statistics (After Compaction) ===' AS info;
SELECT
    getvariable('table_name') AS table_name,
    COUNT(*) AS file_count,
    SUM(file_size_bytes) AS total_size_bytes,
    AVG(file_size_bytes) AS avg_file_size_bytes,
    MIN(file_size_bytes) AS min_file_size_bytes,
    MAX(file_size_bytes) AS max_file_size_bytes
FROM ducklake_table_info_view
WHERE table_name = getvariable('table_name');

-- ============================================================================
-- Show Improved File Distribution
-- ============================================================================
-- Refresh the view to ensure it uses the current table_id
DROP VIEW IF EXISTS ducklake_table_info_view;
CREATE OR REPLACE VIEW ducklake_table_info_view AS
SELECT
    t.table_name,
    df.path AS file_path,
    df.file_size_bytes,
    df.record_count,
    -- Extract partition path from file path if partitioned (simplified)
    CASE
        WHEN df.path LIKE '%year=%' THEN
            REGEXP_EXTRACT(df.path, 'year=[^/]+')
        ELSE NULL
    END AS partition_path
FROM __ducklake_metadata_lake.ducklake_table t
JOIN __ducklake_metadata_lake.ducklake_data_file df ON t.table_id = df.table_id
WHERE t.table_name = getvariable('table_name')
  AND t.table_id = (SELECT original_table_id FROM compaction_context);

SELECT '=== Updated File Distribution ===' AS info;
SELECT
    partition_path,
    COUNT(*) AS file_count,
    SUM(file_size_bytes) AS partition_size_bytes
FROM ducklake_table_info_view
WHERE table_name = getvariable('table_name')
GROUP BY partition_path
ORDER BY file_count DESC
LIMIT 10;

-- ============================================================================
-- Compaction Summary
-- ============================================================================
-- Use the captured before_stats, not the refreshed view
SELECT '=== Compaction Summary ===' AS info;
WITH after_stats AS (
    SELECT COUNT(*) AS after_count, AVG(file_size_bytes) AS after_avg
    FROM ducklake_table_info_view
    WHERE table_name = getvariable('table_name')
)
SELECT
    'File count reduction:' AS metric,
    CAST((SELECT file_count FROM before_compaction_stats) - (SELECT after_count FROM after_stats) AS VARCHAR) AS value
FROM after_stats
UNION ALL
SELECT
    'Average file size increase:' AS metric,
    CAST(ROUND(((SELECT after_avg FROM after_stats) - (SELECT avg_file_size_bytes FROM before_compaction_stats)) / (SELECT avg_file_size_bytes FROM before_compaction_stats) * 100, 2) AS VARCHAR) || '%' AS value
FROM after_stats;

