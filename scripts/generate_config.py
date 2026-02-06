#!/usr/bin/env python3
"""
Generate _config.csv by running _config.sql against the database.

Usage:
    python generate_config.py <page-folder>

Expects:
    <page-folder>/queries/_config.sql

Writes:
    <page-folder>/queries/_config.csv
"""

import argparse
import csv
import sys
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Folder with queries/ subdirectory")
    args = parser.parse_args()

    folder = Path(args.folder)
    sql_file = folder / "queries" / "_config.sql"
    output_file = folder / "queries" / "_config.csv"

    sql = sql_file.read_text().strip()
    if not sql:
        print(f"Error: {sql_file} is empty", file=sys.stderr)
        sys.exit(1)

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(col_names)
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_file}")


if __name__ == "__main__":
    main()
