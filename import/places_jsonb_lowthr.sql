-- Creates places_jsonb_lowthr: identical data to places_jsonb but with
-- toast_tuple_target = 128. Intended to push the data blob out of the heap
-- into TOAST. NOTE: this is a null result by design — it does NOT toast
-- anything. The TOAST machinery only triggers when a tuple exceeds the fixed
-- ~2 KB TOAST_TUPLE_THRESHOLD (page size / 4, a compile-time constant).
-- toast_tuple_target only sets the size the toaster shrinks toward AFTER that
-- trigger fires, and cannot lower the trigger. Every places row is ~1.1-1.2 KB,
-- under 2 KB, so the toaster never runs and this table is byte-identical to
-- places_jsonb. Kept as the evidence behind the article's TOAST section.
--
-- Index set is kept IDENTICAL to places_jsonb so the two tables differ only by
-- the toast_tuple_target reloption — a fair comparison. Dependency: places_jsonb
-- must exist first.

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
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_lowthr_country_idx ON places_jsonb_lowthr ((data->>''primary_country''))'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_lowthr_primary_category_idx ON places_jsonb_lowthr ((data->>''primary_category''))'
);

CALL postgres_execute(
    'pg',
    'CREATE INDEX IF NOT EXISTS places_jsonb_lowthr_confidence_idx ON places_jsonb_lowthr (((data->>''confidence'')::double precision))'
)
