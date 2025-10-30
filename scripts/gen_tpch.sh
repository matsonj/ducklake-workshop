#!/bin/bash
# scripts/gen_tpch.sh
# Purpose: Generate TPCH data using tpchgen-cli
# Usage:   make tpch           (generate all parts)
#          make tpch-part N=1  (generate single part)
set -euo pipefail

# ============================================================================
# Load Environment Variables
# ============================================================================
# Load .env file if it exists (for overriding defaults)
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs) || true
fi

# ============================================================================
# Read Configuration from YAML
# ============================================================================
# Read config values, allowing environment variable overrides
# Defaults come from config/tpch.yaml
SCALE=${TPCH_SCALE:-$(uv run yq -r '.scale' config/tpch.yaml)}
PARTS=${TPCH_PARTS:-$(uv run yq -r '.parts' config/tpch.yaml)}
TABLES=${TPCH_TABLES:-$(uv run yq -r '.tables | join(",")' config/tpch.yaml)}
OUTDIR=$(uv run yq -r '.output_dir' config/tpch.yaml)
ROWGROUP=$(uv run yq -r '.parquet.row_group_bytes' config/tpch.yaml)

# ============================================================================
# Display Configuration
# ============================================================================
echo "TPCH Configuration:"
echo "  Scale Factor: $SCALE"
echo "  Parts: $PARTS"
echo "  Tables: $TABLES"
echo "  Output Directory: $OUTDIR"
echo "  Row Group Size: $ROWGROUP bytes"
echo ""

# ============================================================================
# Create Output Directory
# ============================================================================
mkdir -p "$OUTDIR"

# ============================================================================
# Generate TPCH Data
# ============================================================================
if [ "${1:-all}" = "part" ]; then
    # Generate single part (for incremental demos)
    PART=${2:? "Usage: gen_tpch.sh part <N>"}
    uv run tpchgen-cli \
        -s "$SCALE" \
        --tables "$TABLES" \
        --format parquet \
        --parts "$PARTS" \
        --part "$PART" \
        --parquet-row-group-bytes "$ROWGROUP" \
        --output-dir "$OUTDIR"
else
    # Generate all parts
    uv run tpchgen-cli \
        -s "$SCALE" \
        --tables "$TABLES" \
        --format parquet \
        --parts "$PARTS" \
        --parquet-row-group-bytes "$ROWGROUP" \
        --output-dir "$OUTDIR"
fi

echo "Generated TPCH ($TABLES) scale=$SCALE parts=$PARTS → $OUTDIR"

