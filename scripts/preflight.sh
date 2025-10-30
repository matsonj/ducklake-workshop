#!/bin/bash
# scripts/preflight.sh
# Purpose: Verify all prerequisites are installed and configured
# Usage:   Called automatically by 'make setup'
set -euo pipefail

# ============================================================================
# Helper Function: Check if command exists
# ============================================================================
need() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "Error: Missing required command: $1" >&2
        echo "Please install $1 and ensure it's in your PATH." >&2
        exit 1
    }
}

# ============================================================================
# Verify Required Commands
# ============================================================================
need duckdb
need uv

# ============================================================================
# Set Up Python Virtual Environment
# ============================================================================
echo "Setting up Python virtual environment..."
if [ ! -d .venv ]; then
    uv venv
fi
uv sync

# ============================================================================
# Verify Python Dependencies
# ============================================================================
# Check tpchgen-cli (TPCH data generator)
echo "Checking tpchgen-cli..."
uv run tpchgen-cli --version >/dev/null 2>&1 || {
    echo "Error: tpchgen-cli not available via uv run" >&2
    echo "Try running: uv sync" >&2
    exit 1
}

# Check yq (YAML parser for config file)
echo "Checking yq..."
uv run yq --version >/dev/null 2>&1 || {
    echo "Error: yq not available via uv run" >&2
    echo "Try running: uv sync" >&2
    exit 1
}

# ============================================================================
# Verify DuckLake Extension
# ============================================================================
# DuckLake requires DuckDB 1.3.0+ to be installed
echo "Checking DuckLake extension..."
duckdb -c "INSTALL ducklake; LOAD ducklake; SELECT 'DuckLake extension loaded successfully';" >/dev/null 2>&1 || {
    echo "Warning: Could not load DuckLake extension." >&2
    echo "Make sure DuckDB 1.3.0+ is installed and in your PATH." >&2
    exit 1
}

# ============================================================================
# Check Available Disk Space
# ============================================================================
# TPCH scale 20 with 48 parts requires substantial space
# This is a cheap sanity check (2GB threshold)
FREE=$(df -k . | awk 'NR==2{print $4}')
if [ "$FREE" -lt 2000000 ]; then
    echo "Error: Not enough disk space (~2GB required)." >&2
    echo "Available: $(($FREE / 1024))MB" >&2
    exit 1
fi

echo "Preflight checks passed."

