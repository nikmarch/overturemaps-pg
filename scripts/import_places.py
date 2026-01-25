import os
import duckdb
from pathlib import Path

dsn = (
    f"dbname={os.environ['PGDATABASE']} "
    f"user={os.environ['PGUSER']} "
    f"password={os.environ['PGPASSWORD']} "
    f"host={os.environ['PGHOST']} "
    f"port={os.environ['PGPORT']}"
)

sql_path = Path(__file__).parent / "import_places.sql"
sql = sql_path.read_text()

con = duckdb.connect()
con.execute("INSTALL postgres; LOAD postgres;")
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("SET s3_region = 'us-west-2';")
con.execute(f"ATTACH '{dsn}' AS pg (TYPE postgres)")

for statement in sql.split(";"):
    statement = statement.strip()
    if statement:
        con.execute(statement)

con.close()
