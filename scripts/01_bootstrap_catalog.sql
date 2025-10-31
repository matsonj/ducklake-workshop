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
-- Configure DuckLake Options
-- ============================================================================
-- These options allow for much faster reads & writes from DuckLake
CALL lake.set_option('per_thread_output', 'true');
CALL lake.set_option('parquet_compression', 'zstd');
CALL lake.set_option('parquet_version', '2');
CALL lake.set_option('parquet_row_group_size', '983040');

-- ============================================================================
-- Create Raw Orders Table Schema
-- ============================================================================
-- Schema matches TPCH specification exactly (no partition columns)
CREATE TABLE IF NOT EXISTS lake.orders_raw (
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

-- ============================================================================
-- Create Partitioned Table Schema
-- ============================================================================
-- Schema matches TPCH specification with additional partition columns (year, month, day)
CREATE OR REPLACE TABLE lake.orders (
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderstatus VARCHAR,
    o_totalprice DECIMAL(15,2),
    o_orderdate DATE,
    o_orderpriority VARCHAR,
    o_clerk VARCHAR,
    o_shippriority INTEGER,
    o_comment VARCHAR,
    year INTEGER,
    month INTEGER,
    day INTEGER
);

-- ============================================================================
-- Configure Partitioning
-- ============================================================================
-- Partition by year and month for Hive-style directory layout: year=YYYY/month=MM/
ALTER TABLE lake.orders SET PARTITIONED BY (year, month);

-- ============================================================================
-- Create Partitioned Lineitem Table Schema
-- ============================================================================
-- Schema matches TPCH specification with additional partition column (year)
-- Partitioned by year extracted from l_shipdate
CREATE OR REPLACE TABLE lake.lineitem (
    l_orderkey BIGINT,
    l_partkey BIGINT,
    l_suppkey BIGINT,
    l_linenumber INTEGER,
    l_quantity DECIMAL(15,2),
    l_extendedprice DECIMAL(15,2),
    l_discount DECIMAL(15,2),
    l_tax DECIMAL(15,2),
    l_returnflag VARCHAR,
    l_linestatus VARCHAR,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct VARCHAR,
    l_shipmode VARCHAR,
    l_comment VARCHAR,
    year INTEGER
);

-- ============================================================================
-- Configure Lineitem Partitioning
-- ============================================================================
-- Partition by year for Hive-style directory layout: year=YYYY/
ALTER TABLE lake.lineitem SET PARTITIONED BY (year);

