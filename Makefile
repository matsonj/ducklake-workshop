.PHONY: catalog tpch repartition verify manifest compact expire-snapshots change-feed time-travel load-small-files clean

# Default target
help:
	@echo "DuckLake TPCH Workshop Commands:"
	@echo "  make catalog              - Initialize DuckLake catalog"
	@echo "  make tpch                 - Generate TPCH data"
	@echo "  make repartition          - Repartition orders table"
	@echo "  make verify               - Verify row counts"
	@echo "  make manifest             - Create snapshot"
	@echo "  make compact TABLE=orders - Compact files (default: lineitem)"
	@echo "  make expire-snapshots     - Expire old snapshots"
	@echo "  make change-feed          - Show changes between snapshots"
	@echo "  make time-travel          - Demonstrate time travel queries"
	@echo "  make load-small-files     - Load files one at a time"
	@echo "  make clean                - Remove all generated data"

# Initialize DuckLake catalog
catalog:
	@mkdir -p catalog
	duckdb -f scripts/bootstrap_catalog.sql

# Generate TPCH data
tpch:
	uv run python scripts/generate_data.py

# Repartition orders table
repartition:
	duckdb -f scripts/repartition_orders.sql

# Verify row counts
verify:
	duckdb -f scripts/verify_counts.sql

# Create snapshot
manifest:
	duckdb -f scripts/make_manifest.sql

# Compact files (default: lineitem)
compact:
	@if [ -z "$(TABLE)" ]; then \
		duckdb -f scripts/compaction.sql; \
	else \
		duckdb -c "SET VARIABLE table_name = '$(TABLE)';" -f scripts/compaction.sql; \
	fi

# Expire snapshots (default: 1 minute)
expire-snapshots:
	@if [ -z "$(OLDER_THAN)" ]; then \
		duckdb -f scripts/expire_snapshots.sql; \
	else \
		duckdb -c "SET VARIABLE older_than = INTERVAL '$(OLDER_THAN)';" -f scripts/expire_snapshots.sql; \
	fi

# Change feed analysis
change-feed:
	@if [ -z "$(TABLE)" ] && [ -z "$(FROM_VERSION)" ] && [ -z "$(TO_VERSION)" ]; then \
		duckdb -f scripts/change_feed.sql; \
	else \
		duckdb -c "SET VARIABLE table_name = '$(or $(TABLE),orders)'; SET VARIABLE from_version = $(or $(FROM_VERSION),NULL); SET VARIABLE to_version = $(or $(TO_VERSION),NULL);" -f scripts/change_feed.sql; \
	fi

# Time travel queries
time-travel:
	duckdb -f scripts/time_travel.sql

# Load small files
load-small-files:
	@if [ -z "$(TABLE)" ]; then \
		uv run scripts/load_small_files.py; \
	else \
		uv run scripts/load_small_files.py --table $(TABLE); \
	fi

# Clean up generated data
clean:
	uv run python scripts/clean.py
