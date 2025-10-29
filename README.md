# DuckLake TPCH Demo

This project demonstrates generating TPCH data, repartitioning it into a partitioned lakehouse layout using **[DuckLake](https://ducklake.select/docs/stable/)**, a DuckDB extension that provides lakehouse capabilities.

## Quick start

```bash
cp .env.example .env            # optional
make setup
make tpch                       # or: make tpch-part N=1 (repeat for N)
make catalog                     # Initialize DuckLake catalog
make repartition                 # Load data into DuckLake partitioned table
make verify                      # Verify row counts
make manifest                    # Create snapshot (metadata in DuckLake)
```

## Explore in DuckDB with DuckLake

```sql
-- Load DuckLake extension
INSTALL ducklake; LOAD ducklake;

-- Attach DuckLake database
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- Query partitioned orders table
SELECT year, month, day, COUNT(*) AS rows
FROM orders GROUP BY 1,2,3 ORDER BY 1,2,3;

-- Time travel: query at a specific snapshot
SELECT * FROM orders AT (VERSION => 1);

-- Query DuckLake metadata directly (no manifest files needed!)
SELECT data_file FROM lake.data_file WHERE table_name = 'orders';
SELECT snapshot_id, created_at FROM lake.snapshot ORDER BY snapshot_id DESC;
```

## Project Structure

```
ducklake-tpch/
  config/
    tpch.yaml                  # scale, parts, tables, paths
  catalog/
    ducklake.ducklake           # DuckLake catalog database (contains all metadata)
    ducklake.ducklake.files/    # DuckLake managed Parquet files
  data/
    tpch/                       # raw tpch parquet (by table)
      orders/
    lake/                       # DuckLake managed partitioned data
      orders/                   # partitioned by year/month/day
  scripts/
    preflight.sh                # env checks (DuckDB, DuckLake, tpchgen-cli)
    gen_tpch.sh                 # wrapper around tpchgen-cli (parts / part)
    repartition_orders.sql      # COPY INTO DuckLake partitioned table
    bootstrap_catalog.sql       # initialize DuckLake catalog & add files
    verify_counts.sql           # counts + sanity checks
    make_manifest.sql           # create DuckLake snapshots (metadata in catalog)
```

## Prerequisites

- **uv** (recommended) - Fast Python package installer. Install via:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- **DuckDB** 1.3.0+ (installed and in PATH) - Required for DuckLake extension
- **Bash** (for scripts)
- **make** (for running tasks)

Dependencies installed automatically:
- **tpchgen-cli** (installed via `uv` during `make setup`)
- **DuckLake extension** (installed automatically via DuckDB)
- **yq** (checked during `make setup`, install manually if needed)

## Configuration

Edit `config/tpch.yaml` or set environment variables:
- `TPCH_SCALE`: Scale factor (default: 1)
- `TPCH_PARTS`: Number of parts to generate (default: 48)
- `TPCH_TABLES`: Comma-separated list of tables (default: orders)

## Makefile Targets

- `make setup` - Install Python dependencies (via uv) and verify prerequisites including DuckLake extension
- `make tpch` - Generate all parts for TPCH tables
- `make tpch-part N=1` - Generate only part N (for incremental demos)
- `make catalog` - Initialize DuckLake catalog database
- `make repartition` - Copy data into DuckLake partitioned table
- `make verify` - Show row counts (raw vs lake)
- `make manifest` - Create DuckLake snapshot (metadata stored in catalog, no external files)
- `make clean` - Remove all generated data and catalog

## DuckLake Catalog

DuckLake manages metadata and provides a lakehouse architecture:

- **Catalog Database**: `catalog/ducklake.ducklake` - Stores table metadata
- **Data Files**: `catalog/ducklake.ducklake.files/` - Managed Parquet files
- **Snapshots**: Time-travel queries via `CREATE SNAPSHOT`
- **Partitioning**: Automatic Hive-style partitioning via `PARTITION BY`
- **Add Files**: Uses `ducklake_add_data_files` to register existing Parquet files without copying

**Zero-Copy File Registration**: The `orders_raw` table uses `ducklake_add_data_files` to register existing Parquet files from `data/tpch/orders/` without duplicating them. DuckLake tracks these files in its metadata, allowing you to query them directly as a table.

**Incremental Loading**: After generating new parts with `make tpch-part N=X`, simply re-run `make catalog` to add the new files. The script is idempotent and safe to re-run.

Query tables directly:
```sql
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;
SELECT * FROM orders WHERE year = 1992;
SELECT COUNT(*) FROM orders_raw;  -- Queries registered files directly
```

## Snapshots and Metadata

DuckLake stores all metadata in its catalog database - **no external manifest files needed!**

Create snapshots for time-travel queries:
```sql
CREATE SNAPSHOT orders_snapshot;
SELECT * FROM orders AT (VERSION => 1);
```

Query DuckLake metadata directly:
```sql
-- List all files in a table
SELECT data_file FROM lake.data_file WHERE table_name = 'orders';

-- List all snapshots
SELECT snapshot_id, created_at FROM lake.snapshot ORDER BY snapshot_id DESC;

-- Get file statistics
SELECT table_name, COUNT(*) as file_count, SUM(row_count) as total_rows
FROM lake.data_file
GROUP BY table_name;
```

## Testing

Run sanity checks:
```bash
make tpch
make verify          # Should show matching counts
make repartition
make verify          # Should still match
make catalog
make manifest
```

## Stretch Ideas

- **Rolling arrival**: Loop `make tpch-part N=k` every 10s to "stream" parts
- **dbt-duckdb**: Create dbt models that select from `read_parquet` over `catalog.tables`
- **Polars check**: Script to `scan_parquet` the lake folder and groupby

