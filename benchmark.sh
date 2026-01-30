#!/bin/bash
#
# Run spatial query benchmarks with cold cache.
# Restarts PostgreSQL between each query for accurate measurements.
#
# Usage:
#   ./benchmark.sh <queries-folder>
#   ./benchmark.sh pages/optimizing-spatial-queries/queries
#

set -e

if [ -z "$1" ]; then
    echo "Usage: ./benchmark.sh <queries-folder>"
    echo ""
    echo "Example:"
    echo "  ./benchmark.sh pages/optimizing-spatial-queries/queries"
    exit 1
fi

QUERIES_FOLDER="$1"
CONTAINER_FOLDER="/app/$QUERIES_FOLDER"

if [ ! -d "$QUERIES_FOLDER" ]; then
    echo "Error: Folder not found: $QUERIES_FOLDER"
    exit 1
fi

# Ensure containers are running
if ! docker compose ps --status running | grep -q "server"; then
    echo "Starting containers..."
    docker compose up -d
    sleep 5
fi

# Get config items (array of objects)
CONFIG_JSON=$(docker compose exec -T server python /app/scripts/benchmark.py "$CONTAINER_FOLDER" --config-items)
CONFIG_COUNT=$(echo "$CONFIG_JSON" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))")

if [ "$CONFIG_COUNT" = "0" ]; then
    echo "Error: No items found in _config.json"
    exit 1
fi

echo "Found $CONFIG_COUNT config items"

# Get queries
QUERIES_JSON=$(docker compose exec -T server python /app/scripts/benchmark.py "$CONTAINER_FOLDER" --list)
QUERY_NAMES=$(echo "$QUERIES_JSON" | python3 -c "import sys,json; print(' '.join(q['name'] for q in json.load(sys.stdin)))")

echo "Found queries: $QUERY_NAMES"
echo ""

restart_db() {
    echo -n "    Restarting database... "
    docker compose restart db > /dev/null 2>&1
    for i in $(seq 1 30); do
        if docker compose exec -T db pg_isready -U postgres > /dev/null 2>&1; then
            echo "ready"
            sleep 2
            return 0
        fi
        sleep 1
    done
    echo "FAILED"
    exit 1
}

echo "# Benchmark Results"
echo ""
echo "**Date:** $(date '+%Y-%m-%d %H:%M:%S')"
echo ""
echo "**Method:** Cold cache (PostgreSQL restarted before each query)"
echo ""

# Iterate over config items
echo "$CONFIG_JSON" | python3 -c "
import sys, json
for item in json.load(sys.stdin):
    # Output as JSON string for safe parsing
    print(json.dumps(item))
" | while read -r CONFIG_ITEM; do
    # Extract name for display (use 'name' field if exists, otherwise 'id')
    ITEM_NAME=$(echo "$CONFIG_ITEM" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('name', d.get('id', 'unknown')))")

    echo "## $ITEM_NAME" >&2
    echo "## $ITEM_NAME"
    echo ""
    echo "**Config:** \`$CONFIG_ITEM\`"
    echo ""

    for QUERY_NAME in $QUERY_NAMES; do
        echo "  Running $QUERY_NAME..." >&2

        # Restart for cold cache
        restart_db >&2

        # Run query via Python in Docker
        RESULT=$(docker compose exec -T server python /app/scripts/benchmark.py "$CONTAINER_FOLDER" --query "$QUERY_NAME" --config "$CONFIG_ITEM")

        # Extract values
        TIME_MS=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('time_ms', 'N/A'))")
        COUNT=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count', 'N/A'))")
        EXPLAIN=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('explain', ''))")
        DESC=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('description', ''))")

        echo "    Count: $COUNT, Time: ${TIME_MS}ms" >&2

        echo "### $QUERY_NAME"
        echo ""
        if [ -n "$DESC" ]; then
            echo "$DESC"
            echo ""
        fi
        echo "**Count:** $COUNT"
        echo ""
        echo "**Time:** ${TIME_MS}ms"
        echo ""
        echo "**EXPLAIN ANALYZE:**"
        echo ""
        echo '```'
        echo "$EXPLAIN"
        echo '```'
        echo ""
    done
done

echo "" >&2
echo "Done!" >&2
