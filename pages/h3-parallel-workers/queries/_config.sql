SELECT
  row_number() OVER (ORDER BY n)::int AS id,
  n AS workers_num,
  CASE WHEN n IN (2,3,5,7,11,13,17,19,23,29,31) THEN 'prime' ELSE 'composite' END AS type
FROM unnest(ARRAY[2,3,4,5,6,7,8,13,15,16,21,25,29,35]) AS t(n);
