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


def main():
    print(f"Divisions: {DIVISIONS}")
    print(f"Queries: {list(QUERIES.keys())}\n")

    results = []

    for osm_id in DIVISIONS:
        for name, query in QUERIES.items():
            print(f"{name} on {osm_id}...", flush=True)

            conn = psycopg2.connect(**DB)
            cur = conn.cursor()

            sql = query.format(osm_id=osm_id)

            # Just COUNT with timing
            start = time.time()
            cur.execute(f"SELECT COUNT(*) FROM ({sql}) q")
            count = cur.fetchone()[0]
            elapsed = time.time() - start

            print(f"  {count:,} rows in {elapsed:.1f}s\n", flush=True)

            results.append({
                "osm_id": osm_id,
                "name": name,
                "time_s": elapsed,
                "count": count,
            })

            cur.close()
            conn.close()

    # Write results
    with open(OUTPUT, "w") as f:
        f.write(f"# Benchmark Results\n\nGenerated: {datetime.now().isoformat()}\n\n")
        f.write("| Division | Query | Time | Count |\n")
        f.write("|----------|-------|------|-------|\n")
        for r in results:
            f.write(f"| {r['osm_id']} | {r['name']} | {r['time_s']:.1f}s | {r['count']:,} |\n")

    print(f"Results: {OUTPUT}")


if __name__ == "__main__":
    main()
