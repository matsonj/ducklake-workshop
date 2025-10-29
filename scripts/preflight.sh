#!/bin/bash
# scripts/preflight.sh
set -euo pipefail

need() { 
  command -v "$1" >/dev/null 2>&1 || { 
    echo "Missing $1" >&2
    exit 1
  }
}

need duckdb
need uv

# Set up venv and sync dependencies
echo "Setting up Python virtual environment..."
if [ ! -d .venv ]; then
  uv venv
fi
uv sync

# Verify tpchgen-cli is available via uv run
echo "Checking tpchgen-cli..."
uv run tpchgen-cli --version >/dev/null 2>&1 || {
  echo "Error: tpchgen-cli not available via uv run" >&2
  exit 1
}

# Verify yq is available via uv run
echo "Checking yq..."
uv run yq --version >/dev/null 2>&1 || {
  echo "Error: yq not available via uv run" >&2
  exit 1
}

# Verify DuckLake extension is available
echo "Checking DuckLake extension..."
duckdb -c "INSTALL ducklake; LOAD ducklake; SELECT 'DuckLake extension loaded successfully';" >/dev/null 2>&1 || {
  echo "Warning: Could not load DuckLake extension. Make sure DuckDB 1.3.0+ is installed." >&2
  exit 1
}

# 2GB free space check (cheap sanity)
FREE=$(df -k . | awk 'NR==2{print $4}')
if [ "$FREE" -lt 2000000 ]; then
  echo "Not enough disk space (~>2GB required)." >&2
  exit 1
fi

echo "Preflight OK."

