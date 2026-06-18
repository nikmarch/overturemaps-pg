-- description: Seq-scan aggregation — primitive column GROUP BY vs jsonb key extraction GROUP BY
-- columns: primitive, jsonb, primitive_plan, jsonb_plan
SELECT basic_category, count(*) AS cnt FROM places GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
SELECT data->>'basic_category' AS basic_category, count(*) AS cnt FROM places_jsonb GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
EXPLAIN (ANALYZE, BUFFERS) SELECT basic_category, count(*) AS cnt FROM places GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
EXPLAIN (ANALYZE, BUFFERS) SELECT data->>'basic_category' AS basic_category, count(*) AS cnt FROM places_jsonb GROUP BY 1 ORDER BY 2 DESC LIMIT 20
