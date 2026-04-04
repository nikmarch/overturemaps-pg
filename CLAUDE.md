# Overture Maps PostGIS

## Project

PostGIS + H3 database with Overture Maps data (72M+ places, divisions). DuckDB imports from S3, benchmark framework tests query strategies.

## Stack

- PostgreSQL 17 with PostGIS 3.5, h3, h3_postgis extensions
- DuckDB for S3 → Postgres ETL (`scripts/import.py`)
- Docker Compose: `db` (PostGIS+H3) on port 5434, `server` (Python)
- All commands run inside containers via `docker compose exec`

## Benchmark Framework

Parameterized query runner in `scripts/benchmark.py`. Each experiment lives in `pages/<name>/queries/`.

### Query file format

```sql
-- description: Human-readable explanation of what this query tests.
-- columns: name1, name2, name3
SELECT ...;   -- statement 1 → name1
SELECT ...;   -- statement 2 → name2
SELECT ...    -- statement 3 → name3 (no trailing semicolon on last)
```

- `-- description:` (optional) — appears in markdown report headers
- `-- columns:` — comma-separated names, one per semicolon-separated statement
- `{key}` placeholders are substituted from `_config.csv` columns

### Config

`_config.csv` defines parameter combinations. Each row runs all query files.
`_config.sql` generates the CSV via `./generate_config.sh pages/<name>`.

### Running

```bash
./benchmark.sh pages/<name>              # fresh run
./benchmark.sh pages/<name> --continue   # resume interrupted run
```

### Output

- `results/<timestamp>.csv` — timing + query output per statement
- `results/<timestamp>.md` — human-readable markdown tables

### Import

`scripts/import.py` auto-detects the latest Overture Maps release from S3. SQL files in `import/` use `{release}` placeholder. Tables are skipped if they exist unless `--drop` is passed. `DROP TABLE` uses `CASCADE` to handle dependent materialized views.

## Conventions

- No standalone view files or one-off build scripts — use the benchmark framework
- H3 cells: always derive coarser resolutions via `h3_cell_to_parent()` from r10 base, never compute independently with `h3_latlng_to_cell()` at each resolution (breaks strict hierarchy)
- Use `h3_latlng_to_cell` not `h3_lat_lng_to_cell` (deprecated)
- PostgreSQL `round(double precision, int)` doesn't exist — cast to `::numeric` first
- `psql -v` variables only expand from stdin/file, not `-c` flag
- DDL in benchmark: `conn.autocommit = True`, check `cur.description` before `fetchall()`
