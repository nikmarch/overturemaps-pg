SELECT
  row_number() OVER (ORDER BY t.threshold)::int AS id,
  t.threshold
FROM (VALUES (1000), (5000), (10000), (20000), (50000), (100000)) AS t(threshold)
ORDER BY t.threshold;
