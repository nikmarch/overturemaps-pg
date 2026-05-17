SELECT
  row_number() OVER (ORDER BY n)::int AS id,
  n AS workers_num,
  CASE
    WHEN n IN (2,3,5,7,11,13,17,19,23,29,31) THEN 'prime'
    WHEN n IN (4,8,16,32)                     THEN 'power_of_2'
    ELSE                                           'composite'
  END AS type
FROM unnest(ARRAY[2,3,4,5,6,7,8,11,12,13,16,17,19,20,23,24,29,30,31,32]) AS t(n);
