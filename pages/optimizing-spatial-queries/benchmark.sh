#!/bin/bash
#
# Run spatial query benchmark with cold cache between each query.
# Restarts PostgreSQL before each query for accurate measurements.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
QUERIES_DIR="$SCRIPT_DIR/queries"

mkdir -p "$RESULTS_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULTS_DIR/benchmark_${TIMESTAMP}.md"

cd "$PROJECT_DIR"

restart_db() {
    echo -n "  Restarting database... "
    docker compose restart db > /dev/null 2>&1

    # Wait for PostgreSQL to be ready
    for i in $(seq 1 30); do
        if docker compose exec -T db pg_isready -U postgres > /dev/null 2>&1; then
            echo "ready"
            sleep 2  # Extra wait for stability
            return 0
        fi
        sleep 1
    done

    echo "FAILED"
    exit 1
}

run_query() {
    local query_file="$1"
    local division_id="$2"

    # Load query template and substitute division_id
    local query
    query=$(sed "s/{division_id}/$division_id/g" "$query_file")

    # Run with timing
    echo "\\timing on
$query" | docker compose exec -T db psql -U postgres -d overturemaps 2>&1
}

run_explain() {
    local query_file="$1"
    local division_id="$2"

    # Load query template, substitute, and convert to EXPLAIN ANALYZE
    local query
    query=$(sed "s/{division_id}/$division_id/g" "$query_file" | sed 's/SELECT COUNT(\*)/SELECT */g')

    echo "EXPLAIN ANALYZE
$query" | docker compose exec -T db psql -U postgres -d overturemaps 2>&1
}

parse_time() {
    # Extract time in ms from psql output like "Time: 1234.567 ms"
    grep "^Time:" | head -1 | awk '{print $2}'
}

parse_count() {
    # Extract count from psql output
    grep -E "^\s*[0-9]+$" | head -1 | tr -d ' '
}

format_time() {
    local ms="$1"
    if [ -z "$ms" ]; then
        echo "N/A"
        return
    fi

    awk -v ms="$ms" 'BEGIN {
        if (ms < 1000) {
            printf "%.0fms", ms
        } else if (ms < 60000) {
            printf "%.1fs", ms/1000
        } else {
            minutes = int(ms / 60000)
            seconds = (ms - minutes * 60000) / 1000
            printf "%d:%05.2f", minutes, seconds
        }
    }'
}

echo "Starting benchmark at $(date)"
echo "Results will be saved to: $RESULT_FILE"
echo ""

# Initialize results file
cat > "$RESULT_FILE" << EOF
# Benchmark Results

**Date:** $(date "+%Y-%m-%d %H:%M:%S")

**Method:** Cold cache (PostgreSQL restarted before each query)

## Summary

| Country | Places | JOIN | CTE | Simplified | Best |
|---------|--------|------|-----|------------|------|
EOF

# Temp file for collecting detailed results
DETAILS_FILE=$(mktemp)

# Run benchmarks
run_benchmark() {
    local name="$1"
    local division_id="$2"

    echo "=== $name ==="

    local join_count join_time join_explain
    local cte_count cte_time cte_explain
    local simplified_count simplified_time simplified_explain

    # Run JOIN
    echo "  Running join..."
    restart_db
    local output
    output=$(run_query "$QUERIES_DIR/join.sql" "$division_id")
    join_count=$(echo "$output" | parse_count)
    join_time=$(echo "$output" | parse_time)
    echo "    Count: $join_count, Time: $(format_time "$join_time")"
    join_explain=$(run_explain "$QUERIES_DIR/join.sql" "$division_id")

    # Run CTE
    echo "  Running cte..."
    restart_db
    output=$(run_query "$QUERIES_DIR/cte.sql" "$division_id")
    cte_count=$(echo "$output" | parse_count)
    cte_time=$(echo "$output" | parse_time)
    echo "    Count: $cte_count, Time: $(format_time "$cte_time")"
    cte_explain=$(run_explain "$QUERIES_DIR/cte.sql" "$division_id")

    # Run simplified
    echo "  Running simplified..."
    restart_db
    output=$(run_query "$QUERIES_DIR/simplified.sql" "$division_id")
    simplified_count=$(echo "$output" | parse_count)
    simplified_time=$(echo "$output" | parse_time)
    echo "    Count: $simplified_count, Time: $(format_time "$simplified_time")"
    simplified_explain=$(run_explain "$QUERIES_DIR/simplified.sql" "$division_id")

    echo ""

    # Find best time
    local best
    best=$(awk -v j="$join_time" -v c="$cte_time" -v s="$simplified_time" 'BEGIN {
        min = j; name = "JOIN"
        if (c < min) { min = c; name = "CTE" }
        if (s < min) { min = s; name = "Simplified" }
        print name
    }')

    # Format count with commas
    local formatted_count
    formatted_count=$(printf "%'d" "$join_count" 2>/dev/null || echo "$join_count")

    # Write summary row
    echo "| $name | $formatted_count | $(format_time "$join_time") | $(format_time "$cte_time") | $(format_time "$simplified_time") | $best |" >> "$RESULT_FILE"

    # Write detailed results to temp file
    cat >> "$DETAILS_FILE" << EOF

## $name

**Division ID:** \`$division_id\`

### join

Direct JOIN without CTE.

**Count:** $(printf "%'d" "$join_count" 2>/dev/null || echo "$join_count")

**Time:** $(format_time "$join_time")

**EXPLAIN ANALYZE:**

\`\`\`
$join_explain
\`\`\`

### cte

Using MATERIALIZED CTE.

**Count:** $(printf "%'d" "$cte_count" 2>/dev/null || echo "$cte_count")

**Time:** $(format_time "$cte_time")

**EXPLAIN ANALYZE:**

\`\`\`
$cte_explain
\`\`\`

### simplified

Using MATERIALIZED CTE with ST_Simplify + ST_Buffer.

**Count:** $(printf "%'d" "$simplified_count" 2>/dev/null || echo "$simplified_count")

**Time:** $(format_time "$simplified_time")

**EXPLAIN ANALYZE:**

\`\`\`
$simplified_explain
\`\`\`
EOF
}

# Run all benchmarks
run_benchmark "Argentina" "6aaadea0-9c48-4af0-a47f-bbe020540580"
run_benchmark "Deutschland" "d95d3f8a-a2f4-4436-b0e4-a3a86d15008e"
run_benchmark "United States" "50a03a2c-3e24-4740-b80d-f933ea60c64f"

# Append detailed results
cat "$DETAILS_FILE" >> "$RESULT_FILE"
rm "$DETAILS_FILE"

echo "Results saved to: $RESULT_FILE"
