#!/usr/bin/env python3
"""
Benchmark script for spatial query optimization article.
Compares slow vs fast (simplified polygon) queries across multiple divisions.

Usage:
    python run.py                           # Default divisions
    python run.py --restart                 # Restart PostgreSQL between queries
    python run.py --divisions r51477,r167454
"""

import argparse
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

import psycopg2

# Database connection from environment
DB_CONFIG = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": os.environ.get("PGPORT", "5432"),
    "database": os.environ.get("PGDATABASE", "overturemaps"),
    "user": os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}

# Default divisions to test
DEFAULT_DIVISIONS = ["r51477", "r167454", "r60189"]

# Queries
QUERY_SLOW = """
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT p.id::text, p.name, p.geography as geom
FROM places p
JOIN divisions d ON ST_Covers(d.geography, p.geography)
WHERE d.osm_id = %s;
"""

QUERY_FAST = """
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH div AS MATERIALIZED (
    SELECT ST_Simplify(ST_Buffer(geography::geometry, 0.01), 0.01)::geography as geog
    FROM divisions
    WHERE osm_id = %s
)
SELECT p.id::text, p.name, p.geography as geom
FROM places p, div
WHERE ST_Covers(div.geog, p.geography);
"""


def restart_postgres():
    """Restart PostgreSQL by restarting the db container."""
    print("    Restarting PostgreSQL...")
    subprocess.run(
        ["docker", "compose", "restart", "db"],
        cwd="/app",
        capture_output=True,
        check=True,
    )
    # Wait for PostgreSQL to be ready
    for _ in range(30):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.close()
            print("    PostgreSQL is ready")
            return
        except psycopg2.OperationalError:
            time.sleep(2)
    raise Exception("PostgreSQL did not become ready")


def get_division_info(cursor, osm_id):
    """Get division name and vertex count."""
    cursor.execute(
        """
        SELECT name, ST_NPoints(geography::geometry) as vertices
        FROM divisions
        WHERE osm_id = %s
        LIMIT 1
        """,
        (osm_id,),
    )
    row = cursor.fetchone()
    if row:
        return {"name": row[0] or osm_id, "vertices": row[1]}
    return {"name": osm_id, "vertices": 0}


def run_query(cursor, query, osm_id):
    """Run EXPLAIN ANALYZE query and extract results."""
    cursor.execute(query, (osm_id,))
    rows = cursor.fetchall()
    plan = "\n".join(row[0] for row in rows)

    # Extract execution time
    exec_time_ms = None
    for line in plan.split("\n"):
        if "Execution Time:" in line:
            try:
                exec_time_ms = float(line.split(":")[1].strip().replace(" ms", ""))
            except (ValueError, IndexError):
                pass

    return {"plan": plan, "time_ms": exec_time_ms}


def format_time(ms):
    """Format milliseconds nicely."""
    if ms is None:
        return "N/A"
    if ms < 1000:
        return f"{ms:.0f}ms"
    if ms < 60000:
        return f"{ms/1000:.1f}s"
    minutes = int(ms // 60000)
    seconds = (ms % 60000) / 1000
    return f"{minutes}m {seconds:.0f}s"


def main():
    parser = argparse.ArgumentParser(description="Benchmark spatial queries")
    parser.add_argument(
        "--restart",
        action="store_true",
        help="Restart PostgreSQL between queries for cold cache",
    )
    parser.add_argument(
        "--divisions",
        type=str,
        help="Comma-separated osm_ids (default: r51477,r167454,r102740)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(Path(__file__).parent / "benchmark_results.md"),
        help="Output file path",
    )
    args = parser.parse_args()

    divisions = (
        args.divisions.split(",") if args.divisions else DEFAULT_DIVISIONS
    )

    print("=" * 60)
    print("Spatial Query Benchmark")
    print("=" * 60)
    print(f"Divisions: {', '.join(divisions)}")
    print(f"Restart between queries: {args.restart}")
    print(f"Output: {args.output}")

    results = []

    for osm_id in divisions:
        print(f"\n{'='*60}")
        print(f"Benchmarking: {osm_id}")
        print("=" * 60)

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Get division info
        info = get_division_info(cursor, osm_id)
        print(f"  Division: {info['name']}")
        print(f"  Vertices: {info['vertices']:,}")

        result = {
            "osm_id": osm_id,
            "name": info["name"],
            "vertices": info["vertices"],
        }

        # Run slow query
        if args.restart:
            cursor.close()
            conn.close()
            restart_postgres()
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

        print("\n  Running SLOW query...")
        slow = run_query(cursor, QUERY_SLOW, osm_id)
        result["slow_time_ms"] = slow["time_ms"]
        result["slow_plan"] = slow["plan"]
        print(f"  Time: {format_time(slow['time_ms'])}")

        # Run fast query
        if args.restart:
            cursor.close()
            conn.close()
            restart_postgres()
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

        print("\n  Running FAST query...")
        fast = run_query(cursor, QUERY_FAST, osm_id)
        result["fast_time_ms"] = fast["time_ms"]
        result["fast_plan"] = fast["plan"]
        print(f"  Time: {format_time(fast['time_ms'])}")

        # Calculate improvement
        if slow["time_ms"] and fast["time_ms"] and fast["time_ms"] > 0:
            result["improvement"] = slow["time_ms"] / fast["time_ms"]
            print(f"\n  Improvement: {result['improvement']:.1f}x faster")

        results.append(result)
        cursor.close()
        conn.close()

    # Write results
    write_results(results, args.output)

    print(f"\n{'='*60}")
    print("Benchmark complete!")
    print(f"Results: {args.output}")
    print("=" * 60)


def write_results(results, output_path):
    """Write results to markdown file."""
    with open(output_path, "w") as f:
        f.write("# Spatial Query Benchmark Results\n\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n\n")

        # Summary table
        f.write("## Summary\n\n")
        f.write("| Division | Vertices | Slow | Fast | Improvement |\n")
        f.write("|----------|----------|------|------|-------------|\n")

        for r in results:
            slow = format_time(r.get("slow_time_ms"))
            fast = format_time(r.get("fast_time_ms"))
            improvement = f"{r.get('improvement', 0):.1f}x" if r.get("improvement") else "N/A"
            f.write(f"| {r['name']} | {r['vertices']:,} | {slow} | {fast} | {improvement} |\n")

        # Detailed results
        f.write("\n## Detailed Results\n")

        for r in results:
            f.write(f"\n### {r['name']} ({r['osm_id']})\n\n")
            f.write(f"- **Vertices**: {r['vertices']:,}\n")
            f.write(f"- **Slow query**: {format_time(r.get('slow_time_ms'))}\n")
            f.write(f"- **Fast query**: {format_time(r.get('fast_time_ms'))}\n")
            if r.get("improvement"):
                f.write(f"- **Improvement**: {r['improvement']:.1f}x\n")

            f.write("\n#### Slow Query EXPLAIN\n\n```\n")
            f.write(r.get("slow_plan", "N/A"))
            f.write("\n```\n")

            f.write("\n#### Fast Query EXPLAIN\n\n```\n")
            f.write(r.get("fast_plan", "N/A"))
            f.write("\n```\n")

    print(f"\nResults written to: {output_path}")


if __name__ == "__main__":
    main()
