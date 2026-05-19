CALL postgres_execute('pg', 'DROP TABLE IF EXISTS places CASCADE');

CALL postgres_execute('pg', '
    CREATE TABLE IF NOT EXISTS places (
        id                   text PRIMARY KEY,
        geometry             geometry(Point, 4326),
        name                 text,
        basic_category       text,
        primary_category     text,
        alternate_categories text[],
        confidence           double precision,
        primary_country      text,
        primary_region       text,
        primary_locality     text,
        primary_postcode     text,
        brand_name           text,
        brand_wikidata       text,
        websites             text[],
        socials              text[],
        emails               text[],
        phones               text[],
        names_full           jsonb,
        sources              jsonb,
        bbox                 jsonb
    )
');

INSERT INTO pg.public.places (
    id, geometry, name,
    basic_category, primary_category, alternate_categories,
    confidence,
    primary_country, primary_region, primary_locality, primary_postcode,
    brand_name, brand_wikidata,
    websites, socials, emails, phones,
    names_full, sources, bbox
)
SELECT
    id,
    ST_AsHEXWKB(geometry),
    names.primary,
    basic_category,
    categories.primary,
    categories.alternate,
    confidence,
    addresses[1].country,
    addresses[1].region,
    addresses[1].locality,
    addresses[1].postcode,
    brand.names.primary,
    brand.wikidata,
    websites,
    socials,
    emails,
    phones,
    CAST(to_json(names) AS JSON),
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
    'CREATE INDEX IF NOT EXISTS places_primary_category_idx ON places (primary_category)'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_country_region_idx ON places (primary_country, primary_region)'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_confidence_idx ON places (confidence)'
);

CALL postgres_execute(
    'pg',
    'CREATE EXTENSION IF NOT EXISTS pg_trgm'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_name_trgm_idx ON places USING GIN (name gin_trgm_ops)'
)
