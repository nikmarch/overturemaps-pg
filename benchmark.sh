#!/bin/bash
#
# Run a benchmark script inside the Docker container
#
# Usage:
#   ./benchmark.sh pages/optimizing-spatial-queries/run.py [args...]
#

set -e

if [ -z "$1" ]; then
    echo "Usage: ./benchmark.sh <script-path> [args...]"
    echo ""
    echo "Example:"
    echo "  ./benchmark.sh pages/optimizing-spatial-queries/run.py"
    echo "  ./benchmark.sh pages/optimizing-spatial-queries/run.py --restart"
    exit 1
fi

SCRIPT_PATH="$1"
shift

# Convert local path to container path (pages/foo -> /app/pages/foo)
CONTAINER_PATH="/app/${SCRIPT_PATH}"

# Check if script exists
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "ERROR: Script not found: $SCRIPT_PATH"
    exit 1
fi

# Check if containers are running
if ! docker compose ps --status running | grep -q "server"; then
    echo "Container not running. Starting..."
    docker compose up -d

    echo "Waiting for PostgreSQL to be ready..."
    until docker compose exec -T db pg_isready -U postgres > /dev/null 2>&1; do
        sleep 1
    done
fi

echo "Running: $CONTAINER_PATH $*"
echo ""

docker compose exec -t server python "$CONTAINER_PATH" "$@"
