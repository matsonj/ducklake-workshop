ENV ?= .env

.PHONY: help
help:
	@echo "Targets:"
	@echo "  setup           Install Python dependencies (uv) and verify env"
	@echo "  tpch            Generate all parts for TPCH tables"
	@echo "  tpch-part N=1   Generate only part N (incremental demo)"
	@echo "  repartition     Repartition orders → Hive layout"
	@echo "  catalog         Bootstrap thin catalog"
	@echo "  verify          Show row counts raw vs lake"
	@echo "  manifest        Create DuckLake snapshot (metadata stored in catalog)"
	@echo "  clean           Remove data and catalog (DANGER)"

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

.PHONY: tpch
tpch:
	@bash scripts/gen_tpch.sh all

.PHONY: tpch-part
tpch-part:
	@test -n "$(N)" || (echo "Usage: make tpch-part N=3"; exit 1)
	@bash scripts/gen_tpch.sh part $(N)

.PHONY: repartition
repartition:
	@duckdb < scripts/repartition_orders.sql

.PHONY: catalog
catalog:
	@duckdb < scripts/bootstrap_catalog.sql

.PHONY: verify
verify:
	@duckdb < scripts/verify_counts.sql

.PHONY: manifest
manifest:
	@duckdb < scripts/make_manifest.sql
	@echo "Snapshot created. Query DuckLake metadata directly - no external manifest files needed!"

.PHONY: clean
clean:
	rm -rf data/tpch data/lake catalog/ducklake.ducklake catalog/ducklake.ducklake.files

