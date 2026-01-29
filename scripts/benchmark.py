#!/usr/bin/env python3
"""
Benchmark runner for spatial queries.
Parses SQL file with labeled queries, restarts PostgreSQL between runs for cold cache.

Usage:
    python scripts/benchmark.py pages/optimizing-spatial-queries/benchmark.sql
"""

import subprocess
import sys
import time
import re
from datetime import datetime
from pathlib import Path


# Test divisions: (name, id)
DIVISIONS = [
    ("Argentina", "6aaadea0-9c48-4af0-a47f-bbe020540580"),
    ("Deutschland", "d95d3f8a-a2f4-4436-b0e4-a3a86d15008e"),
    ("United States", "50a03a2c-3e24-4740-b80d-f933ea60c64f"),
]


def parse_sql_file(path: Path) -> list[dict]:
    """Parse SQL file into labeled queries."""
    content = path.read_text()
    queries = []

    # Split by @query markers
    parts = re.split(r'--\s*@query\s+(\w+)\s*\n', content)

    # parts[0] is header, then alternating: name, sql, name, sql...
    for i in range(1, len(parts), 2):
        name = parts[i]
        sql = parts[i + 1].strip() if i + 1 < len(parts) else ""

        # Extract description (first comment line after @query)
        desc_match = re.match(r'--\s*(.+?)\n', sql)
        description = desc_match.group(1) if desc_match else ""

        # Remove leading comment lines from SQL
        sql = re.sub(r'^--.*\n', '', sql).strip()

        if sql:
            queries.append({
                "name": name,
                "description": description,
                "sql": sql,
            })

    return queries


def restart_db() -> bool:
    """Restart PostgreSQL container and wait for ready."""
    print("    Restarting database...", end=" ", flush=True)

    subprocess.run(
        ["docker", "compose", "restart", "db"],
        capture_output=True,
        check=True,
    )

    # Wait for ready
    for _ in range(30):
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "db", "pg_isready", "-U", "postgres"],
            capture_output=True,
        )
        if result.returncode == 0:
            print("ready")
            time.sleep(2)  # Extra stability wait
            return True
        time.sleep(1)

    print("FAILED")
    return False


def run_query(sql: str) -> dict:
    """Run a query and return timing and count."""
    timed_sql = f"\\timing on\n{sql}"

    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "db", "psql", "-U", "postgres", "-d", "overturemaps"],
        input=timed_sql,
        capture_output=True,
        text=True,
    )

    output = result.stdout + result.stderr

    # Parse count
    count = None
    for line in output.split("\n"):
        line = line.strip()
        if line.isdigit():
            count = int(line)
            break

    # Parse time (e.g., "Time: 1234.567 ms")
    time_ms = None
    time_match = re.search(r'Time:\s+([\d.]+)\s*ms', output)
    if time_match:
        time_ms = float(time_match.group(1))

    return {"count": count, "time_ms": time_ms, "output": output}


def run_explain(sql: str) -> str:
    """Run EXPLAIN ANALYZE on a query."""
    # Convert COUNT(*) to * for EXPLAIN
    explain_sql = sql.replace("COUNT(*)", "*")
    explain_sql = f"EXPLAIN ANALYZE\n{explain_sql}"

    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "db", "psql", "-U", "postgres", "-d", "overturemaps"],
        input=explain_sql,
        capture_output=True,
        text=True,
    )

    return result.stdout


def format_time(ms: float | None) -> str:
    """Format milliseconds as human-readable."""
    if ms is None:
        return "N/A"
    if ms < 1000:
        return f"{ms:.0f}ms"
    elif ms < 60000:
        return f"{ms/1000:.1f}s"
    else:
        minutes = int(ms // 60000)
        seconds = (ms % 60000) / 1000
        return f"{minutes}:{seconds:05.2f}"


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/benchmark.py <sql-file>")
        print("Example: python scripts/benchmark.py pages/optimizing-spatial-queries/benchmark.sql")
        sys.exit(1)

    sql_path = Path(sys.argv[1])
    if not sql_path.exists():
        print(f"Error: File not found: {sql_path}")
        sys.exit(1)

    # Parse queries
    queries = parse_sql_file(sql_path)
    print(f"Found {len(queries)} queries: {[q['name'] for q in queries]}")
    print()

    # Setup results directory
    results_dir = sql_path.parent / "results"
    results_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    result_file = results_dir / f"benchmark_{timestamp}.md"

    # Collect results
    all_results = []

    for div_name, div_id in DIVISIONS:
        print(f"=== {div_name} ===")
        div_results = {"name": div_name, "id": div_id, "queries": {}}

        for query in queries:
            print(f"  Running {query['name']}...")

            # Restart for cold cache
            if not restart_db():
                sys.exit(1)

            # Substitute division_id
            sql = query["sql"].replace("{division_id}", div_id)

            # Run query
            result = run_query(sql)
            print(f"    Count: {result['count']}, Time: {format_time(result['time_ms'])}")

            # Run EXPLAIN (warm cache is fine)
            explain = run_explain(sql)

            div_results["queries"][query["name"]] = {
                "description": query["description"],
                "count": result["count"],
                "time_ms": result["time_ms"],
                "explain": explain,
            }

        all_results.append(div_results)
        print()

    # Write markdown results
    with open(result_file, "w") as f:
        f.write("# Benchmark Results\n\n")
        f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"**SQL File:** `{sql_path}`\n\n")
        f.write("**Method:** Cold cache (PostgreSQL restarted before each query)\n\n")

        # Summary table
        f.write("## Summary\n\n")
        headers = ["Country", "Places"] + [q["name"] for q in queries] + ["Best"]
        f.write("| " + " | ".join(headers) + " |\n")
        f.write("|" + "|".join(["---"] * len(headers)) + "|\n")

        for r in all_results:
            times = {name: q["time_ms"] for name, q in r["queries"].items()}
            best_name = min(times, key=lambda k: times[k] or float("inf"))

            # Get count from first query
            first_query = list(r["queries"].values())[0]
            count = first_query["count"]
            count_str = f"{count:,}" if count else "N/A"

            row = [r["name"], count_str]
            for q in queries:
                t = r["queries"][q["name"]]["time_ms"]
                time_str = format_time(t)
                if q["name"] == best_name:
                    time_str = f"**{time_str}**"
                row.append(time_str)
            row.append(best_name)

            f.write("| " + " | ".join(row) + " |\n")

        f.write("\n")

        # Detailed results
        for r in all_results:
            f.write(f"## {r['name']}\n\n")
            f.write(f"**Division ID:** `{r['id']}`\n\n")

            for query in queries:
                q = r["queries"][query["name"]]
                f.write(f"### {query['name']}\n\n")
                if q["description"]:
                    f.write(f"{q['description']}\n\n")

                count_str = f"{q['count']:,}" if q["count"] else "N/A"
                f.write(f"**Count:** {count_str}\n\n")
                f.write(f"**Time:** {format_time(q['time_ms'])}\n\n")
                f.write("**EXPLAIN ANALYZE:**\n\n")
                f.write("```\n")
                f.write(q["explain"].strip())
                f.write("\n```\n\n")

    print(f"Results saved to: {result_file}")


if __name__ == "__main__":
    main()
