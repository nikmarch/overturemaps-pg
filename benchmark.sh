#!/bin/bash
#
# Run a benchmark script inside the Docker container
#
# Usage:
#   ./benchmark.sh pages/optimizing-spatial-queries/benchmark.sql
#

set -e

if [ -z "$1" ]; then
    echo "Usage: ./benchmark.sh <script-path> [args...]"
    echo ""
    echo "Examples:"
    echo "  ./benchmark.sh pages/optimizing-spatial-queries/benchmark.sql"
    echo "  ./benchmark.sh pages/some-other/script.py --args"
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

# Create results directory next to the script
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")
RESULTS_DIR="${SCRIPT_DIR}/results"
mkdir -p "$RESULTS_DIR"

# Generate log filename with timestamp
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
SCRIPT_NAME=$(basename "$SCRIPT_PATH" | sed 's/\.[^.]*$//')
LOG_FILE="${RESULTS_DIR}/${SCRIPT_NAME}_${TIMESTAMP}.log"

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
echo "Logging to: $LOG_FILE"
echo ""

if [[ "$SCRIPT_PATH" == *.sql ]]; then
    docker compose exec -T db psql -U postgres -d overturemaps < "$SCRIPT_PATH" 2>&1 | tee "$LOG_FILE"
else
    docker compose exec -t server python "$CONTAINER_PATH" "$@" 2>&1 | tee "$LOG_FILE"
fi

echo ""
echo "Results saved to: $LOG_FILE"
