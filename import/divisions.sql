CALL postgres_execute('pg', 'DROP TABLE IF EXISTS divisions');

CALL postgres_execute('pg', '
    CREATE TABLE IF NOT EXISTS divisions (
        id text PRIMARY KEY,
        geometry geometry(MultiPolygon, 4326),
        osm_id text,
        name text
    )
');

INSERT INTO pg.public.divisions (id, geometry, osm_id, name)
SELECT
    id,
    geometry,
    split_part(sources[1].record_id, '@', 1),
    names.primary
FROM read_parquet('s3://overturemaps-us-west-2/release/2026-01-21.0/theme=divisions/type=division_area/*');

CALL postgres_execute('pg', 'CREATE INDEX IF NOT EXISTS divisions_geometry_idx ON divisions USING GIST (geometry)')
