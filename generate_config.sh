#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "Usage: ./generate_config.sh <page-folder>"
    echo "Example: ./generate_config.sh pages/optimizing-spatial-queries"
    exit 1
fi

docker compose run --rm --build server python3 /app/scripts/generate_config.py "$1"
