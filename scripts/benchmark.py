#!/usr/bin/env python3
"""
Benchmark runner - substitutes config values into SQL, runs with timing.

Usage:
    python benchmark.py <page-folder>

Expects:
    <page-folder>/queries/_config.json
    <page-folder>/queries/*.sql

Writes results to:
    <page-folder>/results/benchmark_<timestamp>.md
"""

import argparse
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

import psycopg2


def get_db_connection():
    """Get a connection to the database."""
    return psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="postgres",
        dbname="overturemaps",
    )


def wait_for_db():
    """Wait for database to be ready."""
    for _ in range(60):
        result = subprocess.run(
            ["docker", "exec", "overturemaps-pg-db-1", "pg_isready", "-U", "postgres"],
            capture_output=True,
        )
        if result.returncode == 0:
            time.sleep(2)
            return
        time.sleep(1)
    raise RuntimeError("Database failed to become ready")


def restart_db():
    """Restart PostgreSQL via docker and wait for ready."""
    print("restarting db...", end=" ", flush=True)
    subprocess.run(["docker", "restart", "overturemaps-pg-db-1"], capture_output=True, check=True)
    wait_for_db()
    print("ready.", end=" ", flush=True)


def run_query(sql: str) -> tuple[float, str]:
    """Run SQL via psycopg2 and return (time_ms, output)."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        start = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000

        # Format output similar to psql
        if cur.description:
            col_names = [desc[0] for desc in cur.description]
            output_lines = [" | ".join(col_names)]
            output_lines.append("-+-".join("-" * len(name) for name in col_names))
            for row in rows:
                output_lines.append(" | ".join(str(val) for val in row))
            output = "\n".join(output_lines)
        else:
            output = ""

        return elapsed_ms, output
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Folder with queries/ and results/ subdirectories")
    args = parser.parse_args()

    folder = Path(args.folder)
    queries_dir = folder / "queries"
    results_dir = folder / "results"

    config_items = json.loads((queries_dir / "_config.json").read_text())
    sql_files = sorted(queries_dir.glob("*.sql"))

    results_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_file = results_dir / f"benchmark_{timestamp}.md"

    lines = [
        "# Benchmark Results",
        "",
        f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "**Method:** Cold cache (PostgreSQL restarted before each query)",
        "",
    ]

    for config in config_items:
        name = config.get("name", config.get("id", "unknown"))
        print(f"## {name}")
        lines.extend([f"## {name}", "", f"**Config:** `{json.dumps(config)}`", ""])

        for sql_file in sql_files:
            raw_sql = sql_file.read_text().strip()

            # Substitute {key} placeholders
            sql = raw_sql
            for key, value in config.items():
                sql = sql.replace(f"{{{key}}}", str(value))

            print(f"  {sql_file.stem}: ", end="", flush=True)
            restart_db()
            elapsed_ms, output = run_query(sql)
            print(f"{elapsed_ms:.1f}ms")

            lines.extend([
                f"### {sql_file.stem}",
                "",
                "```sql",
                sql,
                "```",
                "",
                f"**Time:** {elapsed_ms:.1f}ms",
                "",
                "```",
                output,
                "```",
                "",
            ])

    output_file.write_text("\n".join(lines))
    print(f"Done! Saved to {output_file}")


if __name__ == "__main__":
    main()
