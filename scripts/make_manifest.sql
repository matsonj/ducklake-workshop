-- scripts/make_manifest.sql
-- Demonstrate DuckLake metadata and snapshot capabilities
INSTALL ducklake; LOAD ducklake;

-- Attach DuckLake database
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- Show current tables in the lake
SELECT '=== Current Tables ===' as info;
SELECT name as table_name FROM (SHOW ALL TABLES) WHERE database = 'lake' ORDER BY name;

-- Show row counts for each table
SELECT '=== Table Row Counts ===' as info;
SELECT 'orders_raw' as table_name, COUNT(*) as row_count FROM orders_raw
UNION ALL
SELECT 'orders' as table_name, COUNT(*) as row_count FROM orders;

-- Query DuckLake metadata tables directly
-- Note: Metadata tables are in the __ducklake_metadata_lake schema
SELECT '=== Available Metadata Tables ===' as info;
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema LIKE '%ducklake%' OR table_name LIKE '%ducklake%'
ORDER BY table_name;

-- Query data files metadata
SELECT '=== Data Files ===' as info;
SELECT t.table_name, df.path, df.record_count 
FROM __ducklake_metadata_lake.ducklake_data_file df
JOIN __ducklake_metadata_lake.ducklake_table t ON df.table_id = t.table_id
ORDER BY t.table_name, df.path 
LIMIT 10;

-- Query snapshots if any exist
SELECT '=== Snapshots ===' as info;
SELECT snapshot_id, snapshot_time 
FROM __ducklake_metadata_lake.ducklake_snapshot 
ORDER BY snapshot_id DESC 
LIMIT 10;

-- Note: All metadata is stored in DuckLake catalog - query directly for file lists, snapshots, etc.
-- See DuckLake documentation for snapshot creation syntax and other metadata tables
