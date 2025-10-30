-- scripts/fix_schema.sql
-- Purpose: Fix schema issues by dropping and recreating orders_raw table
-- Usage:   Run manually if schema mismatches occur
--
-- This is a recovery script for cases where the orders_raw table schema
-- doesn't match the TPCH specification. Normally, bootstrap_catalog.sql
-- handles schema creation correctly.

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
-- Drop and Recreate Table with Correct Schema
-- ============================================================================
-- Drop existing table if schema is incorrect
DROP TABLE IF EXISTS orders_raw;

-- Recreate with correct TPCH specification schema
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

-- ============================================================================
-- Re-register Parquet Files
-- ============================================================================
CALL ducklake_add_data_files('lake', 'orders_raw', 'data/tpch/orders/*.parquet');

