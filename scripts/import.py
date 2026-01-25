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


def execute_sql(con, sql):
    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            con.execute(statement)


def main():
    sql_dir = Path(__file__).parent / "sql"

    if len(sys.argv) > 1:
        sql_files = [sql_dir / f"{sys.argv[1]}.sql"]
    else:
        sql_files = sorted(sql_dir.glob("*.sql"))

    con = duckdb.connect()
    execute_sql(con, SETUP)

    for sql_path in sql_files:
        print(f"Running {sql_path.name}...")
        execute_sql(con, sql_path.read_text())
        print(f"Done {sql_path.name}")

    con.close()


if __name__ == "__main__":
    main()
