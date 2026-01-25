CALL postgres_execute('pg', 'DROP TABLE IF EXISTS places');

CALL postgres_execute('pg', '
    CREATE TABLE places (
        id text PRIMARY KEY,
        geometry geometry(Geometry,4326),
        name text
    )
');

INSERT INTO pg.public.places (id, geometry, name)
SELECT
    id,
    geometry,
    names.primary
FROM read_parquet('s3://overturemaps-us-west-2/release/2026-01-21.0/theme=places/type=place/*')
