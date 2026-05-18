-- description: Worker load distribution for {workers_num} workers ({type}). Measures cell and place balance across workers using h3 index modulo assignment.
-- columns: stats
WITH per_worker AS (
  SELECT
    cell::bigint % {workers_num} AS worker_id,
    count(*)::bigint              AS cell_count,
    sum(place_count)              AS place_count
  FROM places_h3_t10000
  GROUP BY worker_id
)
SELECT
  {workers_num}::int                                                        AS workers_num,
  '{type}'                                                                  AS type,
  round(avg(place_count)::numeric, 0)                                       AS avg_places,
  round(stddev(place_count)::numeric, 0)                                    AS stddev_places,
  round((stddev(place_count) / avg(place_count))::numeric, 4)               AS cv_places,
  min(place_count)                                                          AS min_places,
  max(place_count)                                                          AS max_places,
  round((max(place_count)::numeric / avg(place_count)::numeric)::numeric, 3) AS imbalance_ratio,
  round(avg(cell_count)::numeric, 1)                                        AS avg_cells,
  round(stddev(cell_count)::numeric, 1)                                     AS stddev_cells
FROM per_worker
