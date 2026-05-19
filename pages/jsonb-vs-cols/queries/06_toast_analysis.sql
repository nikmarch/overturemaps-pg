-- description: TOAST impact — same filter on default toast (2040 bytes) vs aggressive toast (128 bytes)
-- columns: toast_sizes, jsonb_default, jsonb_lowthr
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
    (SELECT reloptions FROM pg_class WHERE relname = 'places_jsonb_lowthr');
SELECT count(*) FROM places_jsonb       WHERE data->>'primary_country' = '{country}' AND data->>'basic_category' = '{category}';
SELECT count(*) FROM places_jsonb_lowthr WHERE data->>'primary_country' = '{country}' AND data->>'basic_category' = '{category}'
