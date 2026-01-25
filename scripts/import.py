import os
import sys
import duckdb
from pathlib import Path

dsn = (
    f"dbname={os.environ['PGDATABASE']} "
    f"user={os.environ['PGUSER']} "
    f"password={os.environ['PGPASSWORD']} "
    f"host={os.environ['PGHOST']} "
    f"port={os.environ['PGPORT']}"
)

sql_dir = Path(__file__).parent / "sql"

if len(sys.argv) > 1:
    sql_files = [sql_dir / f"{sys.argv[1]}.sql"]
else:
    sql_files = sorted(sql_dir.glob("*.sql"))

con = duckdb.connect()
con.execute("INSTALL postgres; LOAD postgres;")
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_region = 'us-west-2';")
con.execute("SET enable_progress_bar = true;")
con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres)")
con.execute("SET pg_null_byte_replacement = '';")

for sql_path in sql_files:
    print(f"Running {sql_path.name}...")
    sql = sql_path.read_text()
    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            con.execute(statement)
    print(f"Done {sql_path.name}")

con.close()
