-- description: Gini coefficient for place distribution across {workers_num} workers ({type}). 0 = perfectly uniform, 1 = all places on one worker.
-- columns: gini
SELECT
  round((1 - sum(cumulative_share) * 2.0 / count(*))::numeric, 6) AS gini_places
FROM (
  SELECT place_count,
    sum(place_count) OVER (ORDER BY place_count) * 1.0
      / sum(place_count) OVER () AS cumulative_share
  FROM (
    SELECT
      cell::bigint % {workers_num} AS worker_id,
      sum(place_count)             AS place_count
    FROM places_h3_t10000
    GROUP BY worker_id
  ) w
) g
