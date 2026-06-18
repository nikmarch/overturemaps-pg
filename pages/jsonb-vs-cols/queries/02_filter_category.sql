-- description: Count by category={category} — primitive btree vs jsonb expression index vs low-TOAST-threshold jsonb
-- columns: primitive, jsonb, jsonb_lowthr, primitive_plan, jsonb_plan, jsonb_lowthr_plan
SELECT count(*) FROM places WHERE basic_category = '{category}';
SELECT count(*) FROM places_jsonb WHERE data->>'basic_category' = '{category}';
SELECT count(*) FROM places_jsonb_lowthr WHERE data->>'basic_category' = '{category}';
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM places WHERE basic_category = '{category}';
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM places_jsonb WHERE data->>'basic_category' = '{category}';
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM places_jsonb_lowthr WHERE data->>'basic_category' = '{category}'
