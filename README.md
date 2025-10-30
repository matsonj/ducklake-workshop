# DuckLake TPCH Demo

This project demonstrates generating TPCH data and managing it using **[DuckLake](https://ducklake.select/docs/stable/)**, a DuckDB extension that provides lakehouse capabilities.

## Quick Start

### Using Makefile (Unix/macOS)

```bash
make catalog              # Initialize DuckLake catalog
make tpch                 # Generate TPCH data
make repartition          # Load data into DuckLake partitioned table
make verify               # Verify row counts
make manifest             # Create snapshot
```

### Using DuckDB CLI Directly (Windows/All Platforms)

```bash
duckdb -f scripts/bootstrap_catalog.sql
uv run python generate_data.py
duckdb -f scripts/repartition_orders.sql
duckdb -f scripts/verify_counts.sql
duckdb -f scripts/make_manifest.sql
```

## Project Structure

```
ducklake-tpch/
  config/
    tpch.yaml                  # Configuration for TPCH generation
  catalog/
    ducklake.ducklake           # DuckLake catalog database (contains all metadata)
    ducklake.ducklake.files/    # DuckLake managed Parquet files
  data/
    tpch/                       # Raw TPCH Parquet files (by table)
      orders/
      lineitem/
    lake/                       # DuckLake managed partitioned data
      orders/                   # Partitioned by year/month/day
      lineitem/                 # Partitioned by year
  scripts/
    *.sql                       # SQL scripts executed via duckdb -f
  generate_data.py              # TPCH data generation script
  load_small_files.py           # Load files one at a time script
  Makefile                      # Convenience wrapper (Unix/macOS)
```

## Prerequisites

- **DuckDB** 1.4.0+ (installed and in PATH) - Required for DuckLake extension
- **Python 3.9+** - Required for data generation scripts
- **uv** (recommended) - Fast Python package installer:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```

Dependencies are automatically installed via `uv` when running Python scripts:
- `tpchgen-cli` - TPCH data generator
- `pyyaml` - YAML configuration parsing
- `duckdb` - DuckDB Python package

## Configuration

Edit `config/tpch.yaml` or set environment variables:
- `TPCH_SCALE`: Scale factor (default: 10)
- `TPCH_PARTS`: Number of parts to generate (default: 100)
- `TPCH_TABLES`: Comma-separated list of tables (default: orders,lineitem)

## Available Commands

### Using Makefile (Unix/macOS)

```bash
make help                  # Show all available commands
make catalog               # Initialize DuckLake catalog
make tpch                  # Generate TPCH data
make repartition           # Repartition orders table
make verify                # Verify row counts
make manifest              # Create snapshot
make compact TABLE=orders  # Compact files (default: lineitem)
make expire-snapshots OLDER_THAN="7 days"  # Expire old snapshots
make change-feed           # Show changes between snapshots
make time-travel           # Demonstrate time travel queries
make load-small-files TABLE=lineitem  # Load files one at a time
make clean                 # Remove all generated data
```

### Using DuckDB CLI Directly (All Platforms)

**Basic Commands:**
```bash
# Initialize catalog
duckdb -f scripts/bootstrap_catalog.sql

# Repartition orders table
duckdb -f scripts/repartition_orders.sql

# Verify row counts
duckdb -f scripts/verify_counts.sql

# Create snapshot
duckdb -f scripts/make_manifest.sql

# Time travel queries
duckdb -f scripts/time_travel.sql
```

**Commands with Variables:**
```bash
# Compact files (default: lineitem)
duckdb -f scripts/compaction.sql

# Compact specific table
duckdb -c "SET VARIABLE table_name = 'orders';" -f scripts/compaction.sql

# Expire snapshots (default: 1 minute)
duckdb -f scripts/expire_snapshots.sql

# Expire snapshots older than 7 days
duckdb -c "SET VARIABLE older_than = INTERVAL '7 days';" -f scripts/expire_snapshots.sql

# Change feed analysis (default: orders, latest two snapshots)
duckdb -f scripts/change_feed.sql

# Change feed with specific snapshots
duckdb -c "SET VARIABLE from_version = 5; SET VARIABLE to_version = 6;" -f scripts/change_feed.sql
```

### Python Scripts (All Platforms)

```bash
# Generate TPCH data
uv run python generate_data.py

# Generate specific part
uv run python generate_data.py --part 1

# Load files one at a time
uv run python load_small_files.py

# Load files for specific table
uv run python load_small_files.py --table orders
```

## DuckDB Variables

SQL files use DuckDB's `SET VARIABLE` and `getvariable()` for parameterization:

```sql
-- Set variable with default
SET VARIABLE table_name = 'lineitem';

-- Use variable in query
SELECT * FROM lake.orders WHERE table_name = getvariable('table_name');
```

Override variables before executing SQL files:
```bash
duckdb -c "SET VARIABLE table_name = 'orders';" -f scripts/compaction.sql
```

## Exploring DuckLake

Connect to DuckDB and explore:

```sql
-- Load DuckLake extension
INSTALL ducklake;
LOAD ducklake;

-- Attach DuckLake database
ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- Query partitioned orders table
SELECT year, month, day, COUNT(*) AS rows
FROM orders GROUP BY 1,2,3 ORDER BY 1,2,3;

-- Time travel: query at a specific snapshot
SELECT * FROM orders AT (VERSION => 1);

-- Query DuckLake metadata directly (no manifest files needed!)
SELECT * FROM __ducklake_metadata_lake.ducklake_snapshot;
SELECT * FROM __ducklake_metadata_lake.ducklake_data_file;
SELECT * FROM __ducklake_metadata_lake.ducklake_table;
```

## DuckLake Features Demonstrated

- **Zero-Copy File Registration**: Uses `ducklake_add_data_files` to register existing Parquet files without copying
- **Partitioning**: Automatic Hive-style partitioning (`year=YYYY/month=MM/day=DD`)
- **Snapshots**: Time-travel queries via `CREATE SNAPSHOT` and `AT (VERSION => N)`
- **Metadata Storage**: All metadata in DuckLake catalog database - no external manifest files
- **File Compaction**: Merge adjacent files to improve query performance
- **Change Data Capture**: Identify insertions and deletions between snapshots
- **Snapshot Expiration**: Clean up old snapshots and orphaned files

## SQL Files

All SQL files are in `scripts/` and are executable directly:

- `bootstrap_catalog.sql` - Initialize DuckLake catalog and register files
- `repartition_orders.sql` - Load data into partitioned orders table
- `verify_counts.sql` - Verify row counts match
- `make_manifest.sql` - Create snapshot and show metadata
- `compaction.sql` - Compact small files (uses `table_name` variable)
- `expire_snapshots.sql` - Expire old snapshots (uses `older_than` variable)
- `change_feed.sql` - Show changes between snapshots
- `time_travel.sql` - Demonstrate time travel queries

## Cross-Platform Support

- **Unix/macOS**: Use `make` commands for convenience
- **Windows**: Use `duckdb -f scripts/file.sql` directly
- **All Platforms**: Python scripts work everywhere via `uv run python`

## Troubleshooting

**DuckDB not found:**
```bash
# Install DuckDB (macOS)
brew install duckdb

# Or download from https://duckdb.org/docs/installation/
```

**DuckLake extension not available:**
- Ensure DuckDB 1.4.0+ is installed
- Check extension is installed: `duckdb -c "INSTALL ducklake; LOAD ducklake;"`

**Python dependencies:**
```bash
# Install dependencies
uv sync

# Verify installation
uv run python generate_data.py --help
```

## License

MIT
