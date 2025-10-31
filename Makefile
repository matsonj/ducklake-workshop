.PHONY: catalog tpch repartition verify manifest compact expire-snapshots change-feed time-travel load-small-files clean

# Default target
help:
	@echo "DuckLake TPCH Workshop Commands:"
	@echo "  make tpch                 - Generate TPCH data"
	@echo "  make catalog              - Initialize DuckLake catalog"
	@echo "  make repartition          - Repartition orders table"
	@echo "  make verify               - Verify row counts"
	@echo "  make manifest             - Create snapshot"
	@echo "  make load-small-files     - Load files one at a time"
	@echo "  make compact			   - Compact files (default: lineitem)"
	@echo "  make time-travel          - Demonstrate time travel queries"
	@echo "  make change-feed          - Show changes between snapshots"
	@echo "  make expire-snapshots     - Expire old snapshots"
	@echo "  make clean                - Remove all generated data"

# Generate TPCH data
tpch:
	uv run python scripts/00_generate_data.py

# Initialize DuckLake catalog
catalog:
	duckdb -f scripts/01_bootstrap_catalog.sql

# Repartition orders table
repartition:
	duckdb -f scripts/02_repartition_orders.sql

# Verify row counts
verify:
	duckdb -f scripts/03_verify_counts.sql

# Create snapshot
manifest:
	duckdb -f scripts/04_make_manifest.sql

# Load small files
load-small-files:
	uv run scripts/05_load_small_files.py

# Compact files (default: lineitem)
compact:
	duckdb -f scripts/06_compaction.sql

# Time travel queries
time-travel:
	duckdb -f scripts/07_time_travel.sql

# Change feed analysis
change-feed:
	duckdb -f scripts/08_change_feed.sql

# Expire snapshots (default: 1 minute)
expire-snapshots:
	duckdb -f scripts/09_expire_snapshots.sql

# Clean up generated data
clean:
	uv run scripts/10_clean.py

run:
	make tpch catalog repartition verify manifest load-small-files time-travel change-feed expire-snapshots compact clean
