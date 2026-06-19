-- description: Table sizes: heap, TOAST, indexes, total for all three layouts
-- columns: places_storage, jsonb_storage, jsonb_lowthr_storage
SELECT
    pg_size_pretty(pg_relation_size('places'))                                                          AS heap,
    pg_size_pretty(pg_total_relation_size((SELECT reltoastrelid FROM pg_class WHERE relname = 'places'))) AS toast,
    pg_size_pretty(pg_indexes_size('places'))                                                           AS indexes,
    pg_size_pretty(pg_total_relation_size('places'))                                                    AS total;
SELECT
    pg_size_pretty(pg_relation_size('places_jsonb'))                                                          AS heap,
    pg_size_pretty(pg_total_relation_size((SELECT reltoastrelid FROM pg_class WHERE relname = 'places_jsonb'))) AS toast,
    pg_size_pretty(pg_indexes_size('places_jsonb'))                                                           AS indexes,
    pg_size_pretty(pg_total_relation_size('places_jsonb'))                                                    AS total;
SELECT
    pg_size_pretty(pg_relation_size('places_jsonb_lowthr'))                                                          AS heap,
    pg_size_pretty(pg_total_relation_size((SELECT reltoastrelid FROM pg_class WHERE relname = 'places_jsonb_lowthr'))) AS toast,
    pg_size_pretty(pg_indexes_size('places_jsonb_lowthr'))                                                           AS indexes,
    pg_size_pretty(pg_total_relation_size('places_jsonb_lowthr'))                                                    AS total
