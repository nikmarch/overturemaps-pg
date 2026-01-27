#!/usr/bin/env python3
"""
Benchmark spatial queries. Runs all query variants against all divisions.

Usage:
    python run.py
    python run.py --restart
"""

import argparse
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path

import psycopg2

DB_CONFIG = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": os.environ.get("PGPORT", "5432"),
    "database": os.environ.get("PGDATABASE", "overturemaps"),
    "user": os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}

DIVISIONS = ["r51477", "r167454", "r60189"]

QUERIES = {
    "join": {
        "explain": """
            EXPLAIN (ANALYZE, BUFFERS)
            SELECT p.id::text, p.name, p.geography as geom
            FROM places p
            JOIN divisions d ON ST_Covers(d.geography, p.geography)
            WHERE d.osm_id = '{osm_id}';
        """,
        "count": """
            SELECT COUNT(*)
            FROM places p
            JOIN divisions d ON ST_Covers(d.geography, p.geography)
            WHERE d.osm_id = '{osm_id}';
        """,
    },
    "cte": {
        "explain": """
            EXPLAIN (ANALYZE, BUFFERS)
            WITH div AS MATERIALIZED (
                SELECT geography as geog
                FROM divisions
                WHERE osm_id = '{osm_id}'
            )
            SELECT p.id::text, p.name, p.geography as geom
            FROM places p, div
            WHERE ST_Covers(div.geog, p.geography);
        """,
        "count": """
            WITH div AS MATERIALIZED (
                SELECT geography as geog
                FROM divisions
                WHERE osm_id = '{osm_id}'
            )
            SELECT COUNT(*)
            FROM places p, div
            WHERE ST_Covers(div.geog, p.geography);
        """,
    },
    "cte_simplified": {
        "explain": """
            EXPLAIN (ANALYZE, BUFFERS)
            WITH div AS MATERIALIZED (
                SELECT ST_Simplify(ST_Buffer(geography::geometry, 0.01), 0.01)::geography as geog
                FROM divisions
                WHERE osm_id = '{osm_id}'
            )
            SELECT p.id::text, p.name, p.geography as geom
            FROM places p, div
            WHERE ST_Covers(div.geog, p.geography);
        """,
        "count": """
            WITH div AS MATERIALIZED (
                SELECT ST_Simplify(ST_Buffer(geography::geometry, 0.01), 0.01)::geography as geog
                FROM divisions
                WHERE osm_id = '{osm_id}'
            )
            SELECT COUNT(*)
            FROM places p, div
            WHERE ST_Covers(div.geog, p.geography);
        """,
    },
}

OUTPUT_FILE = Path(__file__).parent / "benchmark_results.md"


def restart_postgres():
    print("  Restarting PostgreSQL...")
    subprocess.run(["docker", "compose", "restart", "db"], cwd="/app", capture_output=True)
    for _ in range(30):
        try:
            psycopg2.connect(**DB_CONFIG).close()
            return
        except psycopg2.OperationalError:
            time.sleep(2)


def run_explain(cursor, query):
    """Run EXPLAIN query, return (exec_time_ms, plan)."""
    start = time.time()
    cursor.execute(query)
    elapsed = time.time() - start
    plan = "\n".join(row[0] for row in cursor.fetchall())

    exec_time = None
    for line in plan.split("\n"):
        if "Execution Time:" in line:
            try:
                exec_time = float(line.split(":")[1].strip().replace(" ms", ""))
            except:
                pass
    return exec_time, elapsed, plan


def run_count(cursor, query):
    """Run COUNT query, return (count, elapsed_seconds)."""
    start = time.time()
    cursor.execute(query)
    count = cursor.fetchone()[0]
    elapsed = time.time() - start
    return count, elapsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--restart", action="store_true")
    args = parser.parse_args()

    print(f"Divisions: {DIVISIONS}")
    print(f"Queries: {list(QUERIES.keys())}")
    print(f"Restart: {args.restart}\n")

    results = []

    for osm_id in DIVISIONS:
        for query_name, query_data in QUERIES.items():
            print(f"Running {query_name} on {osm_id}...")

            if args.restart:
                restart_postgres()

            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # Run EXPLAIN
            explain_query = query_data["explain"].format(osm_id=osm_id)
            exec_time, explain_elapsed, plan = run_explain(cursor, explain_query)
            print(f"  EXPLAIN: {exec_time/1000:.1f}s (wall: {explain_elapsed:.1f}s)")

            # Run COUNT
            count_query = query_data["count"].format(osm_id=osm_id)
            count, count_elapsed = run_count(cursor, count_query)
            print(f"  COUNT:   {count:,} rows (wall: {count_elapsed:.1f}s)\n")

            results.append({
                "osm_id": osm_id,
                "query": query_name,
                "exec_time_ms": exec_time,
                "explain_elapsed": explain_elapsed,
                "count": count,
                "count_elapsed": count_elapsed,
                "plan": plan,
            })

            cursor.close()
            conn.close()

    # Write results
    with open(OUTPUT_FILE, "w") as f:
        f.write(f"# Benchmark Results\n\nGenerated: {datetime.now().isoformat()}\n\n")
        f.write("## Summary\n\n")
        f.write("| Division | Query | Exec Time | Count | Count Time |\n")
        f.write("|----------|-------|-----------|-------|------------|\n")
        for r in results:
            t = f"{r['exec_time_ms']/1000:.1f}s" if r['exec_time_ms'] else "N/A"
            ct = f"{r['count_elapsed']:.1f}s"
            f.write(f"| {r['osm_id']} | {r['query']} | {t} | {r['count']:,} | {ct} |\n")
        f.write("\n## Details\n")
        for r in results:
            f.write(f"\n### {r['osm_id']} - {r['query']}\n\n")
            f.write(f"- **Exec time**: {r['exec_time_ms']/1000:.1f}s\n" if r['exec_time_ms'] else "")
            f.write(f"- **Row count**: {r['count']:,}\n")
            f.write(f"- **Count time**: {r['count_elapsed']:.1f}s\n")
            f.write(f"\n```\n{r['plan']}\n```\n")

    print(f"Results: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
