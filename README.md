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

**Run everything in sequence:**
```bash
make run                  # Execute all steps from tpch to clean
```

### Using DuckDB CLI Directly (Windows/All Platforms)

```bash
duckdb -f scripts/01_bootstrap_catalog.sql
uv run python scripts/00_generate_data.py
duckdb -f scripts/02_repartition_orders.sql
duckdb -f scripts/03_verify_counts.sql
duckdb -f scripts/04_make_manifest.sql
```

## Project Structure

```
ducklake-tpch/
  config/
    tpch.yaml                  # Configuration for TPCH generation
  catalog/
    ducklake.ducklake           # DuckLake catalog database (contains all metadata)
  data/
    tpch/                       # Raw TPCH Parquet files (by table)
      orders/
      lineitem/
    lake/                       # DuckLake managed partitioned data
      orders/                   # Partitioned by year/month/day
      lineitem/                 # Partitioned by year
  scripts/
    00_generate_data.py         # TPCH data generation script
    01_bootstrap_catalog.sql    # Initialize DuckLake catalog
    02_repartition_orders.sql   # Load data into partitioned table
    03_verify_counts.sql        # Verify row counts
    04_make_manifest.sql        # Create snapshot
    05_load_small_files.py      # Load files one at a time
    06_compaction.sql            # Compact files
    07_time_travel.sql          # Time travel queries
    08_change_feed.sql          # Change feed analysis
    09_expire_snapshots.sql     # Expire old snapshots
    10_clean.py                 # Remove all generated data
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
make tpch                  # Generate TPCH data
make catalog               # Initialize DuckLake catalog
make repartition           # Repartition orders table
make verify                # Verify row counts
make manifest              # Create snapshot
make load-small-files      # Load files one at a time
make compact               # Compact files (default: lineitem)
make time-travel           # Demonstrate time travel queries
make change-feed           # Show changes between snapshots
make expire-snapshots      # Expire old snapshots
make clean                 # Remove all generated data
make run                   # Run all steps in sequence
```

### Using DuckDB CLI Directly (All Platforms)

**Basic Commands:**
```bash
# Initialize catalog
duckdb -f scripts/01_bootstrap_catalog.sql

# Repartition orders table
duckdb -f scripts/02_repartition_orders.sql

# Verify row counts
duckdb -f scripts/03_verify_counts.sql

# Create snapshot
duckdb -f scripts/04_make_manifest.sql

# Time travel queries
duckdb -f scripts/07_time_travel.sql
```

**Commands with Variables:**
```bash
# Compact files (default: lineitem)
duckdb -f scripts/06_compaction.sql

# Compact specific table
duckdb -c "SET VARIABLE table_name = 'orders';" -f scripts/06_compaction.sql

# Expire snapshots (default: 1 minute)
duckdb -f scripts/09_expire_snapshots.sql

# Expire snapshots older than 7 days
duckdb -c "SET VARIABLE older_than = INTERVAL '7 days';" -f scripts/09_expire_snapshots.sql

# Change feed analysis (default: orders, latest two snapshots)
duckdb -f scripts/08_change_feed.sql

# Change feed with specific snapshots
duckdb -c "SET VARIABLE from_version = 5; SET VARIABLE to_version = 6;" -f scripts/08_change_feed.sql
```

### Python Scripts (All Platforms)

```bash
# Generate TPCH data
uv run python scripts/00_generate_data.py

# Generate specific part
uv run python scripts/00_generate_data.py --part 1

# Load files one at a time (default: lineitem)
uv run python scripts/05_load_small_files.py

# Load files for specific table
uv run python scripts/05_load_small_files.py --table orders

# Clean up all generated data
uv run python scripts/10_clean.py
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
duckdb -c "SET VARIABLE table_name = 'orders';" -f scripts/06_compaction.sql
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

## Scripts

All scripts are numbered sequentially in `scripts/` directory:

### Python Scripts
- `00_generate_data.py` - Generate TPCH data using tpchgen-cli
- `05_load_small_files.py` - Load Parquet files one at a time to create small files
- `10_clean.py` - Remove all generated data and catalog

### SQL Scripts
- `01_bootstrap_catalog.sql` - Initialize DuckLake catalog and register files
- `02_repartition_orders.sql` - Load data into partitioned orders table
- `03_verify_counts.sql` - Verify row counts match
- `04_make_manifest.sql` - Create snapshot and show metadata
- `06_compaction.sql` - Compact small files (uses `table_name` variable)
- `07_time_travel.sql` - Demonstrate time travel queries
- `08_change_feed.sql` - Show changes between snapshots
- `09_expire_snapshots.sql` - Expire old snapshots (uses `older_than` variable)

All SQL files are executable directly via `duckdb -f scripts/XX_script.sql`

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
uv run python scripts/00_generate_data.py --help
```

## License

MIT
