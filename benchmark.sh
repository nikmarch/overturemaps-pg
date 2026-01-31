#!/bin/bash
set -e

if [ -z "$1" ]; then
    echo "Usage: ./benchmark.sh <queries-folder>"
    exit 1
fi

python3 scripts/benchmark.py "$1"
