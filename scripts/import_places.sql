CALL postgres_execute('pg', '
    CREATE TABLE IF NOT EXISTS places (
        id text PRIMARY KEY,
        geometry geometry(Point, 4326),
        name text
    )
');

CALL postgres_execute('pg', 'TRUNCATE places');

INSERT INTO pg.public.places (id, geometry, name)
SELECT
    id,
    ST_AsText(geometry),
    names.primary
FROM read_parquet('s3://overturemaps-us-west-2/release/2026-01-21.0/theme=places/type=place/*')
