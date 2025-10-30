-- scripts/bootstrap_catalog.sql
-- Purpose: Initialize DuckLake catalog and register existing Parquet files
-- Usage:   make catalog
--
-- This script performs zero-copy file registration: it adds existing Parquet files
-- from data/tpch/orders/ to the DuckLake catalog without duplicating them.
-- Safe to re-run after generating new parts (e.g., via make tpch-part N=X).

-- ============================================================================
-- Initialize DuckLake Extension
-- ============================================================================
INSTALL ducklake;
LOAD ducklake;

-- ============================================================================
-- Attach DuckLake Catalog Database
-- ============================================================================
-- Creates catalog/ducklake.ducklake (metadata) and catalog/ducklake.ducklake.files/ if needed
-- DATA_PATH specifies where partitioned Parquet files will be stored
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- ============================================================================
-- Create Raw Orders Table Schema
-- ============================================================================
-- Schema matches TPCH specification exactly (no partition columns)
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

-- ============================================================================
-- Register Existing Parquet Files (Zero-Copy)
-- ============================================================================
-- ducklake_add_data_files registers files in the catalog without copying them
-- - Accepts glob patterns (e.g., *.parquet)
-- - Idempotent: safe to re-run when new files arrive
-- - Files remain in their original location (data/tpch/orders/)
CALL ducklake_add_data_files('lake', 'orders_raw', 'data/tpch/orders/*.parquet');

-- Note: The partitioned orders table is created during the repartition step

