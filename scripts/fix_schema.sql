-- scripts/fix_schema.sql
-- Drop and recreate orders_raw table with correct TPCH schema
INSTALL ducklake; LOAD ducklake;

ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- Drop existing table if it has wrong schema
DROP TABLE IF EXISTS orders_raw;

-- Recreate with correct TPCH schema
CREATE TABLE orders_raw (
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

-- Add existing Parquet files to the table
CALL ducklake_add_data_files('lake', 'orders_raw', 'data/tpch/orders/*.parquet');

