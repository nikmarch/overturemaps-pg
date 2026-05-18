-- description: Validates that all worker IDs from 0 to workers_num-1 are assigned at least one cell. Missing IDs mean those workers would be idle.
-- columns: validation
WITH per_worker AS (
  SELECT cell::bigint % {workers_num} AS worker_id
  FROM places_h3_t10000
  GROUP BY worker_id
),
expected AS (
  SELECT generate_series(0, {workers_num} - 1) AS worker_id
)
SELECT
  {workers_num}::int                                                                      AS workers_num,
  '{type}'                                                                                AS type,
  count(e.worker_id)                                                                      AS expected_workers,
  count(p.worker_id)                                                                      AS active_workers,
  count(e.worker_id) - count(p.worker_id)                                                 AS idle_workers,
  CASE WHEN count(p.worker_id) = count(e.worker_id) THEN 'PASS' ELSE 'FAIL' END          AS check_result,
  array_agg(e.worker_id ORDER BY e.worker_id) FILTER (WHERE p.worker_id IS NULL)          AS idle_ids
FROM expected e
LEFT JOIN per_worker p ON e.worker_id = p.worker_id
