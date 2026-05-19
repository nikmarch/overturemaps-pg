-- description: Fetch specific fields by id={sample_id} — narrow primitive fetch vs full jsonb blob then extract
-- columns: primitive_fields, jsonb_fields, primitive_star, jsonb_star
SELECT name, primary_country, primary_region, basic_category, confidence
FROM places WHERE id = '{sample_id}';
SELECT data->>'name', data->>'primary_country', data->>'primary_region', data->>'basic_category', (data->>'confidence')::double precision
FROM places_jsonb WHERE id = '{sample_id}';
SELECT * FROM places WHERE id = '{sample_id}';
SELECT * FROM places_jsonb WHERE id = '{sample_id}'
