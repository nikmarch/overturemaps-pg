CALL postgres_execute('pg', 'DROP TABLE IF EXISTS places CASCADE');

CALL postgres_execute('pg', '
    CREATE TABLE IF NOT EXISTS places (
        id text PRIMARY KEY,
        geometry geometry(Point, 4326),
        name text
    )
');

INSERT INTO pg.public.places (id, geometry, name)
SELECT
    id,
    geometry,
    names.primary
FROM
    read_parquet('s3://overturemaps-us-west-2/release/{release}/theme=places/type=place/*');

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_geometry_idx ON places USING GIST (geometry)'
)
