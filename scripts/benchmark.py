#!/usr/bin/env python3
"""
Benchmark runner - runs inside Docker container.
Executes SQL queries from a folder with timing.

Usage:
    python benchmark.py <folder> --list
        List all queries in the folder

    python benchmark.py <folder> --query <name> --config '{"id": "..."}'
        Run a specific query with config object for substitution

    python benchmark.py <folder> --config-items
        List config items from _config.json
"""

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import psycopg2


DB_CONFIG = {
    "host": os.environ.get("PGHOST", "db"),
    "port": os.environ.get("PGPORT", "5432"),
    "user": os.environ.get("PGUSER", "postgres"),
    "password": os.environ.get("PGPASSWORD", "postgres"),
    "dbname": os.environ.get("PGDATABASE", "overturemaps"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def load_config(folder: Path) -> list:
    """Load _config.json from folder (array of objects)."""
    config_file = folder / "_config.json"
    if config_file.exists():
        return json.loads(config_file.read_text())
    return []


def list_queries(folder: Path) -> list[dict]:
    """List all .sql files in folder with their metadata."""
    queries = []
    for sql_file in sorted(folder.glob("*.sql")):
        content = sql_file.read_text()

        # Extract description from first comment line
        desc_match = re.match(r'^--\s*(.+)', content)
        description = desc_match.group(1) if desc_match else ""

        queries.append({
            "name": sql_file.stem,
            "file": str(sql_file),
            "description": description,
        })
    return queries


def run_query(folder: Path, query_name: str, config: dict) -> dict:
    """Run a query and return timing results."""
    sql_file = folder / f"{query_name}.sql"
    if not sql_file.exists():
        return {"error": f"Query file not found: {sql_file}"}

    # Read SQL
    sql = sql_file.read_text()

    # Extract description
    desc_match = re.match(r'^--\s*(.+)', sql)
    description = desc_match.group(1) if desc_match else ""

    # Substitute all {key} placeholders from config
    for key, value in config.items():
        sql = sql.replace(f"{{{key}}}", str(value))

    # Remove comment lines for cleaner execution
    sql_clean = "\n".join(
        line for line in sql.split("\n")
        if not line.strip().startswith("--")
    ).strip()

    try:
        conn = get_connection()
        cur = conn.cursor()

        # Run with timing
        start = time.perf_counter()
        cur.execute(sql_clean)
        row = cur.fetchone()
        elapsed_ms = (time.perf_counter() - start) * 1000

        count = row[0] if row else None

        # Run EXPLAIN ANALYZE
        explain_sql = sql_clean.replace("COUNT(*)", "*")
        cur.execute(f"EXPLAIN ANALYZE {explain_sql}")
        explain_rows = cur.fetchall()
        explain = "\n".join(row[0] for row in explain_rows)

        conn.close()

        return {
            "query": query_name,
            "description": description,
            "config": config,
            "count": count,
            "time_ms": round(elapsed_ms, 2),
            "explain": explain,
        }

    except Exception as e:
        return {"error": str(e)}


def format_time(ms) -> str:
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
    parser = argparse.ArgumentParser(description="Benchmark SQL queries")
    parser.add_argument("folder", help="Folder containing .sql files")
    parser.add_argument("--list", action="store_true", help="List available queries")
    parser.add_argument("--config-items", action="store_true", help="List items from _config.json")
    parser.add_argument("--query", "-q", help="Query name to run")
    parser.add_argument("--config", "-c", help="Config object as JSON string")

    args = parser.parse_args()
    folder = Path(args.folder)

    if args.config_items:
        config = load_config(folder)
        print(json.dumps(config))
    elif args.list:
        if not folder.exists():
            print(json.dumps({"error": f"Folder not found: {folder}"}))
            return
        queries = list_queries(folder)
        print(json.dumps(queries))
    elif args.query and args.config:
        if not folder.exists():
            print(json.dumps({"error": f"Folder not found: {folder}"}))
            return
        config = json.loads(args.config)
        result = run_query(folder, args.query, config)
        print(json.dumps(result))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
