-- scripts/verify_counts.sql
INSTALL ducklake; LOAD ducklake;

-- Attach DuckLake database
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- raw (using DuckLake table with added files)
SELECT 'raw_orders' AS name, COUNT(*) AS rows
FROM orders_raw;

-- lake (using DuckLake partitioned table)
SELECT 'lake_orders' AS name, COUNT(*) AS rows
FROM orders;

