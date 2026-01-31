#!/usr/bin/env python3
"""
Benchmark runner - substitutes config values into SQL, runs with timing.

Usage:
    python benchmark.py <queries-folder>
"""

import argparse
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path


def ensure_containers_running():
    """Start containers if not running."""
    result = subprocess.run(
        ["docker", "compose", "ps", "--status", "running", "-q"],
        capture_output=True,
        text=True,
    )
    if not result.stdout.strip():
        print("Starting containers...")
        subprocess.run(["docker", "compose", "down"], capture_output=True)
        subprocess.run(["docker", "compose", "up", "-d"], check=True)
        wait_for_db()


def wait_for_db():
    """Wait for database to be ready."""
    for _ in range(60):
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "db", "pg_isready", "-U", "postgres"],
            capture_output=True,
        )
        if result.returncode == 0:
            time.sleep(2)
            return
        time.sleep(1)
    raise RuntimeError("Database failed to become ready")


def restart_db():
    """Restart PostgreSQL via docker compose and wait for ready."""
    print("restarting db...", end=" ", flush=True)
    subprocess.run(["docker", "compose", "restart", "db"], capture_output=True, check=True)
    wait_for_db()
    print("ready.", end=" ", flush=True)


def run_query(sql: str) -> tuple[float, str]:
    """Run SQL via psql and return (time_ms, output)."""
    start = time.perf_counter()
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "db", "psql", "-U", "postgres", "-d", "overturemaps", "-c", sql],
        capture_output=True,
        text=True,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms, result.stdout.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Folder with .sql files and _config.json")
    args = parser.parse_args()

    folder = Path(args.folder)
    ensure_containers_running()
    config_items = json.loads((folder / "_config.json").read_text())
    sql_files = sorted(folder.glob("*.sql"))

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_file = folder / f"benchmark_{timestamp}.md"

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
