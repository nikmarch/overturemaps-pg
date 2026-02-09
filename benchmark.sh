#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "Usage: ./benchmark.sh <page-folder> [--continue]"
    echo "Example: ./benchmark.sh pages/optimizing-spatial-queries"
    echo "         ./benchmark.sh pages/optimizing-spatial-queries --continue"
    exit 1
fi

docker compose run --rm --build server python3 /app/scripts/benchmark.py "$@"
