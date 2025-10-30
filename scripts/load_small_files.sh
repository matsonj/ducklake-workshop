#!/bin/bash
# scripts/load_small_files.sh
# Purpose: Load Parquet files one at a time into DuckLake to create small files for compaction demo
# Usage:   make load-small-files [TABLE=lineitem]
#
# This script loads each Parquet file individually to create many small files,
# which can then be compacted using the merge_adjacent_files function.

set -e

TABLE="${TABLE:-lineitem}"
DATA_DIR="data/tpch/${TABLE}"
CATALOG_DB="catalog/ducklake.ducklake"

echo "Loading small files for table: ${TABLE}"
echo "Reading from: ${DATA_DIR}"

# Check if table directory exists
if [ ! -d "${DATA_DIR}" ]; then
    echo "Error: Directory ${DATA_DIR} does not exist."
    echo "Please generate TPCH data first with: make tpch"
    exit 1
fi

# Check if catalog exists
if [ ! -f "${CATALOG_DB}" ]; then
    echo "Error: DuckLake catalog not found at ${CATALOG_DB}"
    echo "Please initialize catalog first with: make catalog"
    exit 1
fi

# Get list of parquet files, sorted numerically
FILES=$(find "${DATA_DIR}" -name "*.parquet" | sort -V)

if [ -z "${FILES}" ]; then
    echo "Error: No Parquet files found in ${DATA_DIR}"
    exit 1
fi

FILE_COUNT=$(echo "${FILES}" | wc -l | tr -d ' ')
echo "Found ${FILE_COUNT} Parquet files to load"

# Initialize DuckLake and create table if it doesn't exist
echo "Initializing DuckLake and ensuring table exists..."
FIRST_FILE=$(echo "${FILES}" | head -1)

# Check if table already exists and drop it if needed
TABLE_EXISTS=$(duckdb -c "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:${CATALOG_DB}' AS lake (DATA_PATH 'data/lake/'); USE lake; SELECT COUNT(*) FROM (SHOW TABLES) WHERE name = '${TABLE}';" -noheader -csv | tail -1)

if [ "${TABLE_EXISTS}" != "0" ] && [ -n "${TABLE_EXISTS}" ]; then
    echo "Warning: Table ${TABLE} already exists. Dropping it to recreate with small files..."
    duckdb <<EOF
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:${CATALOG_DB}' AS lake (DATA_PATH 'data/lake/');
USE lake;
DROP TABLE IF EXISTS ${TABLE};
EOF
fi

# Create table schema from first file with partition column
duckdb <<EOF
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:${CATALOG_DB}' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- Create table with schema from first file plus year partition column
CREATE TABLE ${TABLE} AS 
SELECT *, CAST(NULL AS INTEGER) AS year 
FROM read_parquet('${FIRST_FILE}') WHERE 1=0;

-- Set partitioning by year (similar to repartition_orders.sql)
ALTER TABLE ${TABLE} SET PARTITIONED BY (year);
EOF

# Load each file individually
COUNTER=0
for file in ${FILES}; do
    COUNTER=$((COUNTER + 1))
    echo "[${COUNTER}/${FILE_COUNT}] Loading $(basename ${file})..."
    
    duckdb <<EOF
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:${CATALOG_DB}' AS lake (DATA_PATH 'data/lake/');
USE lake;

-- Insert file data with year partition extracted from l_shipdate
INSERT INTO ${TABLE}
SELECT 
    *,
    year(l_shipdate) AS year
FROM read_parquet('${file}');
EOF
done

echo ""
echo "Successfully loaded ${FILE_COUNT} files into ${TABLE} table"
echo "Each file created a separate DuckLake file, ready for compaction!"
echo ""
echo "Next steps:"
echo "  1. Create a snapshot: make manifest"
echo "  2. Compact files: make compact"

