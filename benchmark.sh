#!/bin/bash
#
# Run spatial query benchmarks with cold cache.
#
# Usage:
#   ./benchmark.sh <sql-file>
#   ./benchmark.sh pages/optimizing-spatial-queries/benchmark.sql
#

set -e

if [ -z "$1" ]; then
    echo "Usage: ./benchmark.sh <sql-file>"
    echo ""
    echo "Example:"
    echo "  ./benchmark.sh pages/optimizing-spatial-queries/benchmark.sql"
    exit 1
fi

SQL_FILE="$1"

if [ ! -f "$SQL_FILE" ]; then
    echo "Error: File not found: $SQL_FILE"
    exit 1
fi

# Run Python benchmark script on host (needs to control Docker)
python3 scripts/benchmark.py "$SQL_FILE"
