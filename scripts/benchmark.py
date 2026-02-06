#!/usr/bin/env python3
"""
Benchmark runner - substitutes config values into SQL, runs with timing.

Each SQL file may contain multiple semicolon-separated statements (e.g.
EXPLAIN ANALYZE followed by SELECT COUNT(*)). The database is restarted
once per SQL file so the first statement runs against a cold cache while
subsequent statements see a warm cache. Each statement is executed and
timed individually, and all results are written to one timestamped CSV.

Column names come from a `-- columns:` comment on the first line of each
SQL file. If absent, columns are named s1, s2, etc.

Usage:
    python benchmark.py <page-folder>

Expects:
    <page-folder>/queries/_config.csv
    <page-folder>/queries/*.sql

Writes results to:
    <page-folder>/results/results_<timestamp>.csv
"""

import argparse
import csv
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


def split_sql_statements(sql: str) -> list[str]:
    """Split SQL text on semicolons. Safe when files have no semicolons in literals."""
    return [s.strip() for s in sql.split(";") if s.strip()]


def parse_column_names(sql: str, file_stem: str, num_stmts: int) -> list[str]:
    """Parse column names from a `-- columns:` comment on the first line."""
    first_line = sql.split("\n", 1)[0].strip()
    if first_line.startswith("-- columns:"):
        names = [n.strip() for n in first_line.split(":", 1)[1].split(",")]
    else:
        names = [f"s{i}" for i in range(1, num_stmts + 1)]
    base = [f"{file_stem}_{n}" for n in names]
    # Each statement gets a result column and a _time column
    cols = []
    for b in base:
        cols.append(b)
        cols.append(f"{b}_time")
    return cols


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Folder with queries/ and results/ subdirectories")
    args = parser.parse_args()

    folder = Path(args.folder)
    queries_dir = folder / "queries"
    results_dir = folder / "results"

    with open(queries_dir / "_config.csv") as f:
        config_items = list(csv.DictReader(f))
    sql_files = sorted(queries_dir.glob("*.sql"), key=lambda f: f.name)

    # First pass: read SQL files and build CSV header
    sql_contents = {}
    header = ["name"]
    for sql_file in sql_files:
        raw_sql = sql_file.read_text().strip()
        stmts = split_sql_statements(raw_sql)
        col_names = parse_column_names(raw_sql, sql_file.stem, len(stmts))
        sql_contents[sql_file] = (raw_sql, stmts, col_names)
        header.extend(col_names)

    results_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_file = results_dir / f"results_{timestamp}.csv"

    # Second pass: run benchmarks and collect rows
    rows = []
    for config in config_items:
        name = config.get("name", config.get("id", "unknown"))
        print(f"## {name}")
        row = {"name": name}

        for sql_file in sql_files:
            raw_sql, _stmts_template, col_names = sql_contents[sql_file]

            # Substitute {key} placeholders
            rendered = raw_sql
            for key, value in config.items():
                rendered = rendered.replace(f"{{{key}}}", str(value))

            statements = split_sql_statements(rendered)

            print(f"  {sql_file.stem}: ", end="", flush=True)
            restart_db()

            for i, stmt in enumerate(statements):
                elapsed_ms, output = run_query(stmt)
                result_col = col_names[i * 2]
                time_col = col_names[i * 2 + 1]
                print(f"{result_col}={elapsed_ms:.1f}ms ", end="", flush=True)
                row[result_col] = output
                row[time_col] = f"{elapsed_ms:.1f}"

            print()

        rows.append(row)

    # Write CSV
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done! Saved to {output_file}")


if __name__ == "__main__":
    main()
