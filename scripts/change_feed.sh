#!/bin/bash
# scripts/change_feed.sh
# Purpose: Wrapper script to dynamically get snapshot IDs and execute change feed queries
# Usage:   make change-feed

set -e

TABLE="${TABLE:-orders}"

# Get snapshot IDs dynamically - find snapshots where the table exists
# We test each snapshot to see if the table exists at that version
# Start with the two most recent snapshots and work backwards
TO_VERSION=""
FROM_VERSION=""
for v in $(duckdb -c "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/'); USE lake; SELECT snapshot_id FROM __ducklake_metadata_lake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 10;" -noheader -csv | tail -n +2 | head -10); do
    # Skip empty lines and non-numeric values
    if [ -z "$v" ] || ! [[ "$v" =~ ^[0-9]+$ ]]; then
        continue
    fi
    result=$(duckdb -c "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/'); USE lake; SELECT COUNT(*) FROM ${TABLE} AT (VERSION => $v) LIMIT 1;" 2>&1)
    if echo "$result" | grep -q "count_star" && ! echo "$result" | grep -q "Catalog Error\|Invalid Input Error"; then
        if [ -z "$TO_VERSION" ]; then
            TO_VERSION=$v
        elif [ -z "$FROM_VERSION" ]; then
            FROM_VERSION=$v
            break
        fi
    fi
done

# If we didn't find two valid snapshots, use defaults based on available snapshots
if [ -z "$TO_VERSION" ] || [ -z "$FROM_VERSION" ]; then
    # Get the latest two snapshots as fallback
    LATEST=$(duckdb -c "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/'); USE lake; SELECT snapshot_id FROM __ducklake_metadata_lake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1;" -noheader -csv | grep -E '^[0-9]+$' | head -1)
    SECOND_LATEST=$(duckdb -c "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:catalog/ducklake.ducklake' AS lake (DATA_PATH 'data/lake/'); USE lake; SELECT snapshot_id FROM __ducklake_metadata_lake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 2 OFFSET 1;" -noheader -csv | grep -E '^[0-9]+$' | head -1)
    TO_VERSION=${TO_VERSION:-$LATEST}
    FROM_VERSION=${FROM_VERSION:-$SECOND_LATEST}
fi

echo "Using FROM_VERSION=$FROM_VERSION and TO_VERSION=$TO_VERSION"

# Execute the change feed SQL with the dynamically determined snapshot IDs
# Replace placeholder snapshot IDs in the SQL file with actual values
sed "s/__FROM_VERSION__/$FROM_VERSION/g; s/__TO_VERSION__/$TO_VERSION/g; s/__TABLE__/$TABLE/g" scripts/change_feed.sql | duckdb

