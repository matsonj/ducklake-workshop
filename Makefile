# Makefile for DuckLake TPCH Demo
# Provides convenient targets for data generation, catalog management, and verification

ENV ?= .env

# ============================================================================
# Help Target
# ============================================================================
.PHONY: help
help:
	@echo "DuckLake TPCH Demo - Available Targets:"
	@echo ""
	@echo "Setup:"
	@echo "  setup           Install Python dependencies (uv) and verify environment"
	@echo ""
	@echo "Data Generation:"
	@echo "  tpch            Generate all parts for TPCH tables"
	@echo "  tpch-part N=1   Generate only part N (for incremental demos)"
	@echo ""
	@echo "DuckLake Operations:"
	@echo "  catalog         Initialize DuckLake catalog and register Parquet files"
	@echo "  repartition      Repartition orders → Hive-style partitioned layout"
	@echo "  manifest        Create DuckLake snapshot (metadata stored in catalog)"
	@echo ""
	@echo "Verification:"
	@echo "  verify          Show row counts (raw vs lake) to verify data integrity"
	@echo ""
	@echo "Cleanup:"
	@echo "  clean           Remove all generated data and catalog (DANGER)"

# ============================================================================
# Setup Targets
# ============================================================================
.PHONY: setup
setup:
	@echo "Setting up DuckLake TPCH demo..."
	@if ! command -v uv >/dev/null 2>&1; then \
		echo "uv not found. Attempting to install uv..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh || (echo "Failed to install uv. Please install manually:"; echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"; exit 1); \
		if [ -f "$$HOME/.cargo/bin/uv" ]; then \
			export PATH="$$HOME/.cargo/bin:$$PATH"; \
		elif [ -f "$$HOME/.local/bin/uv" ]; then \
			export PATH="$$HOME/.local/bin:$$PATH"; \
		fi; \
	fi
	@bash scripts/preflight.sh

# ============================================================================
# Data Generation Targets
# ============================================================================
.PHONY: tpch
tpch:
	@bash scripts/gen_tpch.sh all

.PHONY: tpch-part
tpch-part:
	@test -n "$(N)" || (echo "Usage: make tpch-part N=3"; exit 1)
	@bash scripts/gen_tpch.sh part $(N)

# ============================================================================
# DuckLake Operation Targets
# ============================================================================
.PHONY: catalog
catalog:
	@duckdb < scripts/bootstrap_catalog.sql

.PHONY: repartition
repartition:
	@duckdb < scripts/repartition_orders.sql

.PHONY: manifest
manifest:
	@duckdb < scripts/make_manifest.sql
	@echo "Snapshot created. Query DuckLake metadata directly - no external manifest files needed!"

# ============================================================================
# Verification Targets
# ============================================================================
.PHONY: verify
verify:
	@duckdb < scripts/verify_counts.sql

# ============================================================================
# Cleanup Targets
# ============================================================================
.PHONY: clean
clean:
	rm -rf data/tpch data/lake catalog/ducklake.ducklake catalog/ducklake.ducklake.files

