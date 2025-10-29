-- scripts/repartition_orders.sql
-- Read raw files → write partitioned lake by orderdate using DuckLake
INSTALL ducklake; LOAD ducklake;

-- Attach DuckLake database
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- Create partitioned table schema in DuckLake (matching TPCH specification)
CREATE OR REPLACE TABLE orders (
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

-- Set partitioning using ALTER TABLE (DuckLake syntax)
ALTER TABLE orders SET PARTITIONED BY (year, month);

-- Copy data from raw orders table into partitioned DuckLake table
-- This reads from the orders_raw table (which has files added via ducklake_add_data_files)
INSERT INTO orders
SELECT *,
       year(o_orderdate)  AS year,
       month(o_orderdate) AS month,
       day(o_orderdate)   AS day
FROM orders_raw
ORDER BY o_orderdate;

