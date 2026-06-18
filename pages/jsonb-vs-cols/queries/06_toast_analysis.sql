-- description: TOAST storage — default toast (2040 bytes) vs aggressive toast (128 bytes). Query-timing comparison lives in 02-05, which now run all three tables.
-- columns: toast_sizes
SELECT
    'places_jsonb'        AS "table",
    current_setting('block_size')                                                                               AS page_bytes,
    pg_size_pretty(pg_relation_size('places_jsonb'))                                                            AS heap,
    pg_size_pretty(pg_total_relation_size((SELECT reltoastrelid FROM pg_class WHERE relname = 'places_jsonb')))  AS toast,
    (SELECT reloptions FROM pg_class WHERE relname = 'places_jsonb')                                            AS storage_opts
UNION ALL
SELECT
    'places_jsonb_lowthr',
    current_setting('block_size'),
    pg_size_pretty(pg_relation_size('places_jsonb_lowthr')),
    pg_size_pretty(pg_total_relation_size((SELECT reltoastrelid FROM pg_class WHERE relname = 'places_jsonb_lowthr'))),
    (SELECT reloptions FROM pg_class WHERE relname = 'places_jsonb_lowthr')
