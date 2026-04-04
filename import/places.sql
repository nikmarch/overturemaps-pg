CALL postgres_execute('pg', 'DROP TABLE IF EXISTS places CASCADE');

CALL postgres_execute('pg', '
    CREATE TABLE IF NOT EXISTS places (
        id text PRIMARY KEY,
        geometry geometry(Point, 4326),
        name text,
        primary_country text,
        primary_region text,
        primary_subregion text,
        primary_locality text,
        basic_category text,
        confidence double precision,
        websites jsonb,
        emails jsonb,
        phones jsonb,
        sources jsonb,
        bbox jsonb
    )
');

INSERT INTO pg.public.places (
    id,
    geometry,
    name,
    primary_country,
    primary_region,
    primary_subregion,
    primary_locality,
    basic_category,
    confidence,
    websites,
    emails,
    phones,
    sources,
    bbox
)
SELECT
    id,
    geometry,
    names.primary,
    address.country,
    address.region,
    address.subregion,
    address.locality,
    basic_category,
    confidence,
    CAST(to_json(websites) AS JSON),
    CAST(to_json(emails) AS JSON),
    CAST(to_json(phones) AS JSON),
    CAST(to_json(sources) AS JSON),
    CAST(to_json(bbox) AS JSON)
FROM
    read_parquet('s3://overturemaps-us-west-2/release/{release}/theme=places/type=place/*');

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_geometry_idx ON places USING GIST (geometry)'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_basic_category_idx ON places (basic_category)'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_primary_country_region_idx ON places (primary_country, primary_region)'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_name_trgm_idx ON places USING GIN (name gin_trgm_ops)'
)
