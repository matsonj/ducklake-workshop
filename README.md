# DuckLake TPCH Demo

This project demonstrates generating TPCH data, repartitioning it into a partitioned lakehouse layout using **[DuckLake](https://ducklake.select/docs/stable/)**, a DuckDB extension that provides lakehouse capabilities.

## Quick start

```bash
cp .env.example .env            # optional
uv run python run.py setup
uv run python run.py tpch                       # or: uv run python run.py tpch --part 1 (repeat for N)
uv run python run.py catalog                     # Initialize DuckLake catalog
uv run python run.py repartition                 # Load data into DuckLake partitioned table
uv run python run.py verify                      # Verify row counts
uv run python run.py manifest                    # Create snapshot (metadata in DuckLake)
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
    *.sql                       # SQL scripts executed by run.py
  run.py                        # Main Python CLI script (replaces Makefile)
```

## Prerequisites

- **Python 3.9+** - Required for running the Python scripts
- **uv** (recommended) - Fast Python package installer. Install via:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- **DuckDB** 1.3.0+ (installed and in PATH) - Required for DuckLake extension

Dependencies installed automatically:
- **tpchgen-cli** (installed via `uv` during `python run.py setup`)
- **DuckLake extension** (installed automatically via DuckDB)
- **yq** (installed via `uv` during setup)
- **duckdb** Python package (installed via `uv` during setup)

## Configuration

Edit `config/tpch.yaml` or set environment variables:
- `TPCH_SCALE`: Scale factor (default: 1)
- `TPCH_PARTS`: Number of parts to generate (default: 48)
- `TPCH_TABLES`: Comma-separated list of tables (default: orders)

## Available Commands

Run `uv run python run.py --help` for full help, or:

- `uv run python run.py setup` - Install Python dependencies (via uv) and verify prerequisites including DuckLake extension
- `uv run python run.py tpch` - Generate all parts for TPCH tables
- `uv run python run.py tpch --part 1` - Generate only part N (for incremental demos)
- `uv run python run.py catalog` - Initialize DuckLake catalog database
- `uv run python run.py repartition` - Copy data into DuckLake partitioned table
- `uv run python run.py verify` - Show row counts (raw vs lake)
- `uv run python run.py manifest` - Create DuckLake snapshot (metadata stored in catalog, no external files)
- `uv run python run.py load-small-files [--table lineitem]` - Load Parquet files one at a time to create small files
- `uv run python run.py compact [--table orders]` - Compact small files to improve query performance
- `uv run python run.py expire-snapshots [--older-than '7 days'] [--dry-run]` - Expire old snapshots and clean up orphaned files
- `uv run python run.py change-feed [--table orders]` - Show row-level changes between snapshots
- `uv run python run.py time-travel` - Demonstrate time travel queries
- `uv run python run.py clean` - Remove all generated data and catalog

## DuckLake Catalog

DuckLake manages metadata and provides a lakehouse architecture:

- **Catalog Database**: `catalog/ducklake.ducklake` - Stores table metadata
- **Data Files**: `catalog/ducklake.ducklake.files/` - Managed Parquet files
- **Snapshots**: Time-travel queries via `CREATE SNAPSHOT`
- **Partitioning**: Automatic Hive-style partitioning via `PARTITION BY`
- **Add Files**: Uses `ducklake_add_data_files` to register existing Parquet files without copying

**Zero-Copy File Registration**: The `orders_raw` table uses `ducklake_add_data_files` to register existing Parquet files from `data/tpch/orders/` without duplicating them. DuckLake tracks these files in its metadata, allowing you to query them directly as a table.

**Incremental Loading**: After generating new parts with `uv run python run.py tpch --part X`, simply re-run `uv run python run.py catalog` to add the new files. The script is idempotent and safe to re-run.

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
uv run python run.py tpch
uv run python run.py verify          # Should show matching counts
uv run python run.py repartition
uv run python run.py verify          # Should still match
uv run python run.py catalog
uv run python run.py manifest
```

## Stretch Ideas

- **Rolling arrival**: Loop `uv run python run.py tpch --part k` every 10s to "stream" parts
- **dbt-duckdb**: Create dbt models that select from `read_parquet` over `catalog.tables`
- **Polars check**: Script to `scan_parquet` the lake folder and groupby

## Cross-Platform Support

This project uses Python scripts instead of Make/bash for better cross-platform compatibility (Windows, macOS, Linux). All SQL remains in separate `.sql` files for transparency and easy inspection.

