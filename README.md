# Overture Maps to PostGIS

Import [Overture Maps](https://overturemaps.org/) data into PostGIS using DuckDB.

## Quick Start

```bash
./start.sh
```

This will:
1. Start PostGIS and the import server
2. Import places and divisions from Overture Maps S3

## Manual Import

```bash
docker compose up -d
docker compose exec -t server python /scripts/import.py           # all
docker compose exec -t server python /scripts/import.py places    # just places
docker compose exec -t server python /scripts/import.py divisions # just divisions
```

## Connect to Database

```bash
docker compose exec db psql -U postgres -d overturemaps
```

## Tables

| Table | Columns | Source |
|-------|---------|--------|
| `places` | id, geography, name | Overture places |
| `divisions` | id, geography, osm_id | Overture division_area |

Uses `geography` type for accurate distance/area calculations in meters. Both tables have GIST indexes on geography.

## Example Queries

```sql
-- Find places within 1km of a point
SELECT name FROM places
WHERE ST_DWithin(geography, ST_MakePoint(-122.4194, 37.7749)::geography, 1000);

-- Calculate area in square meters
SELECT osm_id, ST_Area(geography) as area_m2 FROM divisions LIMIT 5;
```
