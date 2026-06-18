-- description: Compound filter country={country} AND category={category} — composite btree vs two expression indexes
-- columns: primitive, jsonb, primitive_plan, jsonb_plan
SELECT count(*) FROM places WHERE primary_country = '{country}' AND basic_category = '{category}';
SELECT count(*) FROM places_jsonb WHERE data->>'primary_country' = '{country}' AND data->>'basic_category' = '{category}';
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM places WHERE primary_country = '{country}' AND basic_category = '{category}';
EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM places_jsonb WHERE data->>'primary_country' = '{country}' AND data->>'basic_category' = '{category}'
