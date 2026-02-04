#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "Usage: ./benchmark.sh <page-folder>"
    echo "Example: ./benchmark.sh pages/optimizing-spatial-queries"
    exit 1
fi

docker compose run --rm --build server python3 /app/scripts/benchmark.py "$1"
