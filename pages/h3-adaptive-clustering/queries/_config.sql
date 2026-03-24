SELECT
  row_number() OVER (ORDER BY t.threshold)::int AS id,
  t.threshold
FROM (VALUES (5000), (10000), (50000), (100000)) AS t(threshold)
ORDER BY t.threshold;
