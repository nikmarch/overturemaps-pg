import os
import sys
from pathlib import Path

import duckdb

dsn = (
    f"dbname={os.environ['PGDATABASE']} "
    f"user={os.environ['PGUSER']} "
    f"password={os.environ['PGPASSWORD']} "
    f"host={os.environ['PGHOST']} "
    f"port={os.environ['PGPORT']}"
)

SETUP = f"""
    INSTALL postgres; LOAD postgres;
    INSTALL spatial; LOAD spatial;
    INSTALL httpfs; LOAD httpfs;
    SET s3_region = 'us-west-2';
    SET enable_progress_bar = true;
    ATTACH '{dsn}' AS pg (TYPE postgres);
    SET pg_null_byte_replacement = ''
"""


def get_latest_release(con):
    """Find the latest Overture Maps release from S3."""
    rows = con.execute("""
        SELECT replace(file, 's3://overturemaps-us-west-2/release/', '') AS release
        FROM glob('s3://overturemaps-us-west-2/release/*/theme=places/type=place/*')
        LIMIT 1
    """).fetchone()
    if not rows:
        raise RuntimeError("Could not find any Overture Maps release on S3")
    # Extract release version from path like "2026-01-21.0/theme=places/type=place/..."
    return rows[0].split("/")[0]


def execute_sql(con, sql, drop=False):
    for statement in sql.split(";"):
        statement = statement.strip()
        if not statement:
            continue
        if "DROP TABLE" in statement and not drop:
            continue
        con.execute(statement)


def table_exists(con, table):
    result = con.execute(f"""
        SELECT COUNT(*) FROM postgres_query('pg',
            'SELECT 1 FROM information_schema.tables WHERE table_name = ''{table}'''
        )
    """).fetchone()
    return result[0] > 0


def main():
    sql_dir = Path("/app/import")

    args = sys.argv[1:]
    drop = "--drop" in args
    names = [a for a in args if not a.startswith("--")]

    if names:
        sql_files = [sql_dir / f"{name}.sql" for name in names]
    else:
        sql_files = sorted(sql_dir.glob("*.sql"))

    con = duckdb.connect()
    execute_sql(con, SETUP)

    release = get_latest_release(con)
    print(f"Using Overture Maps release: {release}")

    for sql_path in sql_files:
        table = sql_path.stem

        if not drop and table_exists(con, table):
            print(f"Skipping {sql_path.name} (table exists, use --drop to reimport)")
            continue

        print(f"Running {sql_path.name}...")
        sql = sql_path.read_text().replace("{release}", release)
        execute_sql(con, sql, drop=drop)
        print(f"Done {sql_path.name}")

    con.close()


if __name__ == "__main__":
    main()
