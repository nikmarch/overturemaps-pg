CALL postgres_execute('pg', 'DROP TABLE IF EXISTS places_jsonb CASCADE');

CALL postgres_execute('pg', '
    CREATE TABLE IF NOT EXISTS places_jsonb (
        id       text PRIMARY KEY,
        geometry geometry(Point, 4326),
        data     jsonb
    )
');

INSERT INTO pg.public.places_jsonb (id, geometry, data)
SELECT
    id,
    ST_AsHEXWKB(geometry),
    CAST(regexp_replace(CAST(to_json({
        'name':                names.primary,
        'basic_category':      basic_category,
        'primary_category':    categories.primary,
        'alternate_categories': categories.alternate,
        'confidence':          confidence,
        'primary_country':     addresses[1].country,
        'primary_region':      addresses[1].region,
        'primary_locality':    addresses[1].locality,
        'primary_postcode':    addresses[1].postcode,
        'brand_name':          brand.names.primary,
        'brand_wikidata':      brand.wikidata,
        'websites':            websites,
        'socials':             socials,
        'emails':              emails,
        'phones':              phones,
        'names_full':          names,
        'sources':             sources,
        'bbox':                bbox
    }) AS VARCHAR), '\\u0000', '', 'g') AS JSON)
FROM
    read_parquet('s3://overturemaps-us-west-2/release/{release}/theme=places/type=place/*');

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_geometry_idx ON places_jsonb USING GIST (geometry)'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_basic_category_idx ON places_jsonb ((data->>''basic_category''))'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_primary_category_idx ON places_jsonb ((data->>''primary_category''))'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_country_idx ON places_jsonb ((data->>''primary_country''))'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_country_category_idx ON places_jsonb ((data->>''primary_country''), (data->>''basic_category''))'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_confidence_idx ON places_jsonb (((data->>''confidence'')::double precision))'
)
