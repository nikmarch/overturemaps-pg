-- description: Fetch by id={sample_id} — narrow primitive fetch vs jsonb blob extract vs low-TOAST-threshold jsonb
-- columns: primitive_fields, jsonb_fields, jsonb_lowthr_fields, primitive_star, jsonb_star, jsonb_lowthr_star, primitive_plan, jsonb_plan, jsonb_lowthr_plan
SELECT name, primary_country, primary_region, basic_category, confidence
FROM places WHERE id = '{sample_id}';
SELECT data->>'name', data->>'primary_country', data->>'primary_region', data->>'basic_category', (data->>'confidence')::double precision
FROM places_jsonb WHERE id = '{sample_id}';
SELECT data->>'name', data->>'primary_country', data->>'primary_region', data->>'basic_category', (data->>'confidence')::double precision
FROM places_jsonb_lowthr WHERE id = '{sample_id}';
SELECT * FROM places WHERE id = '{sample_id}';
SELECT * FROM places_jsonb WHERE id = '{sample_id}';
SELECT * FROM places_jsonb_lowthr WHERE id = '{sample_id}';
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM places WHERE id = '{sample_id}';
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM places_jsonb WHERE id = '{sample_id}';
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM places_jsonb_lowthr WHERE id = '{sample_id}'
