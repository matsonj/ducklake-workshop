#!/usr/bin/env python3
"""Load Parquet files one at a time to create small files"""
import sys
from pathlib import Path

import duckdb


def execute_statement(statement, setup_parts=None):
    """Execute a single SQL statement with its own connection"""
    print(f"Executing: {statement}")
    conn = duckdb.connect()
    if setup_parts:
        for setup_part in setup_parts:
            conn.execute(setup_part)
    conn.execute(statement)
    conn.close()


def main():
    table = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] != "--table" else "lineitem"
    if len(sys.argv) > 2 and sys.argv[1] == "--table":
        table = sys.argv[2]
    
    data_dir = Path(f"data/tpch/{table}")
    catalog_db = "catalog/ducklake.ducklake"
    
    print(f"Loading small files for table: {table}")
    print(f"Reading from: {data_dir}")
    
    if not data_dir.exists():
        print(f"Error: Directory {data_dir} does not exist.")
        print("Please generate TPCH data first with: uv run python generate_data.py")
        sys.exit(1)
    
    if not Path(catalog_db).exists():
        print(f"Error: DuckLake catalog not found at {catalog_db}")
        print("Please initialize catalog first with: duckdb -f scripts/bootstrap_catalog.sql")
        sys.exit(1)
    
    parquet_files = sorted(data_dir.glob("*.parquet"), key=lambda p: int(p.stem.split(".")[-1]) if p.stem.split(".")[-1].isdigit() else 0)
    
    if not parquet_files:
        print(f"Error: No Parquet files found in {data_dir}")
        sys.exit(1)
    
    setup_parts = ["INSTALL ducklake;", "LOAD ducklake;", f"ATTACH 'ducklake:{catalog_db}' AS lake (DATA_PATH 'data/lake/');"]
    
    execute_statement(f"SELECT COUNT(*) FROM lake.{table} LIMIT 1;", setup_parts)
    
    for idx, file_path in enumerate(parquet_files, 1):
        print(f"[{idx}/{len(parquet_files)}] Loading {file_path.name}...")
        if table == "lineitem":
            insert_sql = f"""INSERT INTO lake.{table}
SELECT 
    l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, 
    l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, 
    l_receiptdate, l_shipinstruct, l_shipmode, l_comment,
    year(l_shipdate) AS year
FROM read_parquet('{str(file_path)}');"""
        elif table == "orders":
            insert_sql = f"""INSERT INTO lake.{table}
SELECT 
    o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, 
    o_orderpriority, o_clerk, o_shippriority, o_comment,
    year(o_orderdate) AS year, month(o_orderdate) AS month, day(o_orderdate) AS day
FROM read_parquet('{str(file_path)}');"""
        else:
            insert_sql = f"INSERT INTO lake.{table} SELECT *, year(l_shipdate) AS year FROM read_parquet('{str(file_path)}');"
        execute_statement(insert_sql, setup_parts)
    
    print(f"\nSuccessfully loaded {len(parquet_files)} files into {table} table")
    print("Each file created a separate DuckLake file, ready for compaction!\n")
    print("Next steps:")
    print("  1. Create a snapshot: duckdb -f scripts/make_manifest.sql")
    print("  2. Compact files: duckdb -c \"SET VARIABLE table_name = 'lineitem';\" -f scripts/compaction.sql")


if __name__ == "__main__":
    main()

