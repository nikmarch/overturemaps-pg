CALL postgres_execute('pg', 'DROP TABLE IF EXISTS divisions');

CALL postgres_execute('pg', '
    CREATE TABLE divisions (
        id text PRIMARY KEY,
        geography geography,
        osm_id text
    )
');

INSERT INTO pg.public.divisions (id, geography, osm_id)
SELECT
    id,
    geometry,
    split_part(sources[1].record_id, '@', 1)
FROM read_parquet('s3://overturemaps-us-west-2/release/2026-01-21.0/theme=divisions/type=division_area/*');

CALL postgres_execute('pg', 'CREATE INDEX divisions_geography_idx ON divisions USING GIST (geography)')
