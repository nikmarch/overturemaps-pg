# Overture Maps to PostGIS

Import [Overture Maps](https://overturemaps.org/) data into PostGIS using DuckDB.

## Requirements

- Docker

## Quick Start

```bash
./start.sh --drop           # fresh import
./start.sh places --drop    # just places
./start.sh divisions        # divisions (skip if exists)
```

## Manual Import

```bash
docker compose up -d
docker compose exec -t server python /scripts/import.py                  # all (skip if exists)
docker compose exec -t server python /scripts/import.py --drop           # all (fresh)
docker compose exec -t server python /scripts/import.py places --drop    # just places (fresh)
```

## Options

| Flag | Description |
|------|-------------|
| `--drop` | Drop and recreate tables before import |

Without `--drop`, import is skipped if the table already exists.

## Database

```bash
docker compose exec db psql -U postgres -d overturemaps
```

Connection: `postgres://postgres:postgres@localhost:5432/overturemaps`

## PostGIS + H3

The `db` container is PostGIS plus the `h3` PostgreSQL extension (via `h3-pg`).

### Notes about existing volumes

Extensions are created automatically only on **first init** (fresh `pgdata` volume) via `/docker-entrypoint-initdb.d/00_extensions.sql`.

If you already have an existing `pgdata` volume, run this once:

```bash
docker compose exec -T db psql -U postgres -d overturemaps -c "CREATE EXTENSION IF NOT EXISTS h3;"
```

## Tables

| Table | Columns | Source |
|-------|---------|--------|
| `places` | id, geography, name | [places](https://docs.overturemaps.org/guides/places/) |
| `divisions` | id, geography, osm_id, name | [divisions](https://docs.overturemaps.org/guides/divisions/) |

Both tables use `geography` type (accurate distance/area in meters) with GIST indexes.

## Adding New Imports

Add a SQL file to `scripts/sql/`:

```sql
CALL postgres_execute('pg', 'DROP TABLE IF EXISTS mytable');

CALL postgres_execute('pg', '
    CREATE TABLE IF NOT EXISTS mytable (
        id text PRIMARY KEY,
        geography geography
    )
');

INSERT INTO pg.public.mytable (id, geography)
SELECT id, geometry
FROM read_parquet('s3://overturemaps-us-west-2/release/2026-01-21.0/theme=...');

CALL postgres_execute('pg', 'CREATE INDEX IF NOT EXISTS mytable_geography_idx ON mytable USING GIST (geography)')
```

Then run: `docker compose exec -t server python /scripts/import.py mytable --drop`
