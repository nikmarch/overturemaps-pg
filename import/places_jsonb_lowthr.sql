-- Creates places_jsonb_lowthr: identical data to places_jsonb but with
-- toast_tuple_target = 128, forcing virtually every row into TOAST.
-- Setting the target BEFORE insert means new rows are TOASTed immediately
-- (no VACUUM FULL needed). Dependency: places_jsonb must exist first.

CALL postgres_execute('pg', 'DROP TABLE IF EXISTS places_jsonb_lowthr CASCADE');

CALL postgres_execute('pg', '
    CREATE TABLE places_jsonb_lowthr (
        id       text PRIMARY KEY,
        geometry geometry(Point, 4326),
        data     jsonb
    ) WITH (toast_tuple_target = 128)
');

CALL postgres_execute('pg', '
    INSERT INTO places_jsonb_lowthr
    SELECT * FROM places_jsonb
');

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_lowthr_geometry_idx ON places_jsonb_lowthr USING GIST (geometry)'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_lowthr_basic_category_idx ON places_jsonb_lowthr ((data->>''basic_category''))'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_lowthr_country_category_idx ON places_jsonb_lowthr ((data->>''primary_country''), (data->>''basic_category''))'
)
