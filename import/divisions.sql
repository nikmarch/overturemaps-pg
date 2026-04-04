CALL postgres_execute('pg', 'DROP TABLE IF EXISTS divisions CASCADE');

CALL postgres_execute('pg', '
    CREATE TABLE IF NOT EXISTS divisions (
        id text PRIMARY KEY,
        geometry geometry(MultiPolygon, 4326),
        osm_id text,
        name text,
        class text,
        subtype text,
        country text,
        region text,
        admin_level integer,
        parent_division_id text,
        sources jsonb,
        bbox jsonb
    )
');

INSERT INTO pg.public.divisions (
    id,
    geometry,
    osm_id,
    name,
    class,
    subtype,
    country,
    region,
    admin_level,
    parent_division_id,
    sources,
    bbox
)
SELECT
    id,
    geometry,
    split_part(sources[1].record_id, '@', 1),
    names.primary,
    class,
    subtype,
    country,
    region,
    admin_level,
    parent_division_id,
    CAST(to_json(sources) AS JSON),
    CAST(to_json(bbox) AS JSON)
FROM
    read_parquet('s3://overturemaps-us-west-2/release/{release}/theme=divisions/type=division_area/*');

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS divisions_geometry_idx ON divisions USING GIST (geometry)'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS divisions_parent_division_idx ON divisions (parent_division_id)'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS divisions_country_region_idx ON divisions (country, region)'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS divisions_admin_level_idx ON divisions (admin_level)'
)
