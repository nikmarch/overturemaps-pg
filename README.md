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

Connection: `postgres://postgres:postgres@localhost:5434/overturemaps`

## PostGIS + H3

The `db` container is PostGIS plus the `h3` PostgreSQL extension (via `h3-pg`).

### Notes about existing volumes

Extensions are created automatically only on **first init** (fresh `pgdata` volume) via `/docker-entrypoint-initdb.d/00_extensions.sql`.

If you already have an existing `pgdata` volume, run this once:

```bash
docker compose exec -T db psql -U postgres -d overturemaps -c "CREATE EXTENSION IF NOT EXISTS h3;"
docker compose exec -T db psql -U postgres -d overturemaps -c "CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;"
```

## Benchmarks

Run parameterized query benchmarks against pages:

```bash
# 1. Generate config (test cases)
./generate_config.sh pages/h3-adaptive-clustering

# 2. Run benchmarks
./benchmark.sh pages/h3-adaptive-clustering

# 3. Resume if interrupted
./benchmark.sh pages/h3-adaptive-clustering --continue
```

Results are saved to `pages/<page>/results/`.

## Tables

| Table | Columns | Source |
|-------|---------|--------|
| `places` | id, geometry, name, primary_country, primary_region, primary_subregion, primary_locality, basic_category, confidence, websites, emails, phones, sources, bbox | [places](https://docs.overturemaps.org/guides/places/) |
| `divisions` | id, geometry, osm_id, name, class, subtype, country, region, admin_level, parent_division_id, sources, bbox | [divisions](https://docs.overturemaps.org/guides/divisions/) |

Both tables use PostGIS `geometry` columns with GIST indexes. Additional btree indexes support category, hierarchy, and region filtering; `places.name` also gets a `pg_trgm` GIN index for fast lexical lookup.

## Suggested next steps

This repo intentionally keeps the raw Overture themes as the main schema surface. The next useful layer should come from views/materialized views first, not by immediately introducing a pile of new tables.

Recommended progression:

1. Keep enriching the base import with stable, query-relevant Overture columns.
2. Add validation queries to profile null rates, subtype distribution, category coverage, and hierarchy completeness per release.
3. Add read-only views for:
   - place-to-division containment
   - division ancestry traversal
   - category rollups by region/admin level
4. Only then decide whether any derived tables are actually justified.
5. Treat `pgvector` as optional and late-stage; start with `pg_trgm` + spatial filters.

## Adding New Imports

Add a SQL file to `import/`:

```sql
CALL postgres_execute('pg', 'DROP TABLE IF EXISTS mytable');

CALL postgres_execute('pg', '
    CREATE TABLE IF NOT EXISTS mytable (
        id text PRIMARY KEY,
        geometry geometry
    )
');

INSERT INTO pg.public.mytable (id, geometry)
SELECT id, geometry
FROM read_parquet('s3://overturemaps-us-west-2/release/{release}/theme=...');

CALL postgres_execute('pg', 'CREATE INDEX IF NOT EXISTS mytable_geometry_idx ON mytable USING GIST (geometry)')
```

Then run: `docker compose exec -t server python /scripts/import.py mytable --drop`
