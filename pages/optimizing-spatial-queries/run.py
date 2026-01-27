#!/usr/bin/env python3
"""
Benchmark spatial queries. Runs all query variants against all divisions.
"""

import os
import time
from datetime import datetime
from pathlib import Path

import psycopg2

DB = {
    "host": os.environ.get("PGHOST", "localhost"),
    "port": os.environ.get("PGPORT", "5432"),
    "database": os.environ.get("PGDATABASE", "overturemaps"),
    "user": os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
}

DIVISIONS = ["r51477", "r167454", "r60189"]

QUERIES = {
    "join": """
        SELECT p.id::text, p.name, p.geography as geom
        FROM places p
        JOIN divisions d ON ST_Covers(d.geography, p.geography)
        WHERE d.osm_id = '{osm_id}'
    """,
    "cte": """
        WITH div AS MATERIALIZED (
            SELECT geography as geog
            FROM divisions
            WHERE osm_id = '{osm_id}'
        )
        SELECT p.id::text, p.name, p.geography as geom
        FROM places p, div
        WHERE ST_Covers(div.geog, p.geography)
    """,
    "cte_simplified": """
        WITH div AS MATERIALIZED (
            SELECT ST_Buffer(ST_Simplify(geography::geometry, 1000 / 111342.0), 1000 / 111342.0)::geography as geog
            FROM divisions
            WHERE osm_id = '{osm_id}'
        )
        SELECT p.id::text, p.name, p.geography as geom
        FROM places p, div
        WHERE ST_Covers(div.geog, p.geography)
    """,
}

OUTPUT = Path(__file__).parent / "benchmark_results.md"


def timed(cursor, sql):
    """Execute SQL and return (result, elapsed_seconds)."""
    start = time.time()
    cursor.execute(sql)
    return cursor.fetchall(), time.time() - start


def parse_exec_time(plan):
    """Extract execution time from EXPLAIN output."""
    for line in plan:
        if "Execution Time:" in line[0]:
            try:
                return float(line[0].split(":")[1].strip().replace(" ms", ""))
            except:
                pass
    return None


def main():
    print(f"Divisions: {DIVISIONS}")
    print(f"Queries: {list(QUERIES.keys())}\n")

    results = []

    for osm_id in DIVISIONS:
        for name, query in QUERIES.items():
            print(f"{name} on {osm_id}...")

            conn = psycopg2.connect(**DB)
            cur = conn.cursor()

            sql = query.format(osm_id=osm_id)

            # EXPLAIN ANALYZE
            plan, t1 = timed(cur, f"EXPLAIN (ANALYZE, BUFFERS) {sql}")
            exec_ms = parse_exec_time(plan)
            print(f"  explain: {exec_ms/1000:.1f}s (wall: {t1:.1f}s)")

            # COUNT
            rows, t2 = timed(cur, f"SELECT COUNT(*) FROM ({sql}) q")
            count = rows[0][0]
            print(f"  count:   {count:,} (wall: {t2:.1f}s)\n")

            results.append({
                "osm_id": osm_id,
                "name": name,
                "exec_ms": exec_ms,
                "count": count,
                "count_time": t2,
                "plan": "\n".join(r[0] for r in plan),
            })

            cur.close()
            conn.close()

    # Write results
    with open(OUTPUT, "w") as f:
        f.write(f"# Benchmark Results\n\nGenerated: {datetime.now().isoformat()}\n\n")
        f.write("## Summary\n\n")
        f.write("| Division | Query | Time | Count |\n")
        f.write("|----------|-------|------|-------|\n")
        for r in results:
            t = f"{r['exec_ms']/1000:.1f}s" if r['exec_ms'] else "N/A"
            f.write(f"| {r['osm_id']} | {r['name']} | {t} | {r['count']:,} |\n")
        f.write("\n## Details\n")
        for r in results:
            f.write(f"\n### {r['osm_id']} - {r['name']}\n\n```\n{r['plan']}\n```\n")

    print(f"Results: {OUTPUT}")


if __name__ == "__main__":
    main()
