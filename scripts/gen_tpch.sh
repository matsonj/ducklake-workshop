#!/bin/bash
# scripts/gen_tpch.sh
set -euo pipefail

# Load .env if it exists
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs) || true
fi

# Use yq to read config file (via uv run)
SCALE=${TPCH_SCALE:-$(uv run yq -r '.scale' config/tpch.yaml)}
PARTS=${TPCH_PARTS:-$(uv run yq -r '.parts' config/tpch.yaml)}
TABLES_CSV=${TPCH_TABLES:-$(uv run yq -r '.tables | join(",")' config/tpch.yaml)}
OUTDIR=$(uv run yq -r '.output_dir' config/tpch.yaml)
ROWGROUP=$(uv run yq -r '.parquet.row_group_bytes' config/tpch.yaml)

echo "TPCH Configuration:"
echo "  Scale Factor: $SCALE"
echo "  Parts: $PARTS"
echo "  Tables: $TABLES_CSV"
echo "  Output Directory: $OUTDIR"
echo "  Row Group Size: $ROWGROUP bytes"
echo ""

mkdir -p "$OUTDIR"

if [ "${1:-all}" = "part" ]; then
  PART=${2:? "Usage: gen_tpch.sh part <N>"}
  uv run tpchgen-cli -s "$SCALE" --tables "$TABLES_CSV" --format parquet \
    --parts "$PARTS" --part "$PART" \
    --parquet-row-group-bytes "$ROWGROUP" \
    --output-dir "$OUTDIR"
else
  uv run tpchgen-cli -s "$SCALE" --tables "$TABLES_CSV" --format parquet \
    --parts "$PARTS" \
    --parquet-row-group-bytes "$ROWGROUP" \
    --output-dir "$OUTDIR"
fi

echo "Generated TPCH ($TABLES_CSV) scale=$SCALE parts=$PARTS → $OUTDIR"

