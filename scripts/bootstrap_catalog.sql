-- scripts/bootstrap_catalog.sql
-- Initialize DuckLake catalog database
INSTALL ducklake; LOAD ducklake;

-- Attach DuckLake database (creates catalog/ducklake.ducklake and catalog/ducklake.ducklake.files)
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- Create raw orders table schema (matching TPCH specification)
CREATE TABLE IF NOT EXISTS orders_raw (
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderstatus VARCHAR,
    o_totalprice DECIMAL(15,2),
    o_orderdate DATE,
    o_orderpriority VARCHAR,
    o_clerk VARCHAR,
    o_shippriority INTEGER,
    o_comment VARCHAR
);

-- Add existing Parquet files to the table (without copying)
-- Note: ducklake_add_data_files accepts glob patterns
-- This is idempotent - safe to re-run if new files arrive
CALL ducklake_add_data_files('lake', 'orders_raw', 'data/tpch/orders/*.parquet');

-- Note: The partitioned orders table will be created during repartition step

