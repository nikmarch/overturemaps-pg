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
    conn.autocommit = True
    try:
        cur = conn.cursor()
        start = time.perf_counter()
        cur.execute(sql)
        elapsed_ms = (time.perf_counter() - start) * 1000

        # Format output similar to psql
        if cur.description:
            rows = cur.fetchall()
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


def find_latest_results(results_dir: Path) -> Path | None:
    """Find the most recent results CSV in the directory."""
    files = sorted(results_dir.glob("results_*.csv"), key=lambda f: f.name, reverse=True)
    return files[0] if files else None


def load_completed_ids(results_file: Path) -> set[str]:
    """Read IDs already present in a results CSV."""
    completed = set()
    with open(results_file) as f:
        for row in csv.DictReader(f):
            completed.add(row["id"])
    return completed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Folder with queries/ and results/ subdirectories")
    parser.add_argument("--continue", dest="continue_run", action="store_true",
                        help="Continue from the latest results file, skipping completed divisions")
    args = parser.parse_args()

    folder = Path(args.folder)
    queries_dir = folder / "queries"
    results_dir = folder / "results"

    with open(queries_dir / "_config.csv") as f:
        config_items = list(csv.DictReader(f))
    sql_files = sorted(
        (f for f in queries_dir.glob("*.sql") if not f.name.startswith("_")),
        key=lambda f: f.name,
    )

    # First pass: read SQL files and build CSV header
    config_fields = list(config_items[0].keys())
    sql_contents = {}
    header = list(config_fields)
    for sql_file in sql_files:
        raw_sql = sql_file.read_text().strip()
        stmts = split_sql_statements(raw_sql)
        col_names = parse_column_names(raw_sql, sql_file.stem, len(stmts))
        sql_contents[sql_file] = (raw_sql, stmts, col_names)
        header.extend(col_names)

    results_dir.mkdir(parents=True, exist_ok=True)

    # Determine output file and completed IDs
    completed_ids: set[str] = set()
    if args.continue_run:
        latest = find_latest_results(results_dir)
        if latest and latest.stat().st_size > 0:
            output_file = latest
            completed_ids = load_completed_ids(latest)
            print(f"Continuing {output_file.name} — {len(completed_ids)} divisions already done, "
                  f"{len(config_items) - len(completed_ids)} remaining")
        else:
            print("No existing results to continue from, starting fresh")
            timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            output_file = results_dir / f"results_{timestamp}.csv"
    else:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        output_file = results_dir / f"results_{timestamp}.csv"

    # Stream rows to CSV as each config completes
    mode = "a" if completed_ids else "w"
    with open(output_file, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if not completed_ids:
            writer.writeheader()

        for config in config_items:
            if config["id"] in completed_ids:
                print(f"## {config.get('name', config['id'])} — skipped (already done)")
                continue
            name = config.get("name", config.get("id", "unknown"))
            print(f"## {name}")
            row = dict(config)

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

            writer.writerow(row)
            f.flush()

    print(f"Done! Saved to {output_file}")


if __name__ == "__main__":
    main()
