-- description: Count by category={category} — btree index on primitive vs expression index on jsonb
-- columns: primitive, jsonb
SELECT count(*) FROM places WHERE basic_category = '{category}';
SELECT count(*) FROM places_jsonb WHERE data->>'basic_category' = '{category}'
