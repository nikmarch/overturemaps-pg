-- description: TOAST storage — primitive vs default-toast jsonb (2040 bytes) vs aggressive-toast jsonb (128 bytes). Query-timing comparison lives in 02-05, which run all three tables.
-- columns: toast_sizes
SELECT
    'places'              AS "table",
    current_setting('block_size')                                                                               AS page_bytes,
    pg_size_pretty(pg_relation_size('places'))                                                                  AS heap,
    pg_size_pretty(pg_total_relation_size((SELECT reltoastrelid FROM pg_class WHERE relname = 'places')))        AS toast,
    (SELECT reloptions FROM pg_class WHERE relname = 'places')                                                  AS storage_opts
UNION ALL
SELECT
    'places_jsonb',
    current_setting('block_size'),
    pg_size_pretty(pg_relation_size('places_jsonb')),
    pg_size_pretty(pg_total_relation_size((SELECT reltoastrelid FROM pg_class WHERE relname = 'places_jsonb'))),
    (SELECT reloptions FROM pg_class WHERE relname = 'places_jsonb')
UNION ALL
SELECT
    'places_jsonb_lowthr',
    current_setting('block_size'),
    pg_size_pretty(pg_relation_size('places_jsonb_lowthr')),
    pg_size_pretty(pg_total_relation_size((SELECT reltoastrelid FROM pg_class WHERE relname = 'places_jsonb_lowthr'))),
    (SELECT reloptions FROM pg_class WHERE relname = 'places_jsonb_lowthr')
