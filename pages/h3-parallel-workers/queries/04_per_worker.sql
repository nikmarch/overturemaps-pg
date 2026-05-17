-- description: Per-worker cell and place totals for {workers_num} workers ({type}). Shows the full distribution across all worker IDs.
-- columns: per_worker
SELECT
  e.worker_id,
  coalesce(count(v.cell), 0)            AS cell_count,
  coalesce(sum(v.place_count), 0)       AS place_count
FROM generate_series(0, {workers_num} - 1) AS e(worker_id)
LEFT JOIN places_h3_t10000 v ON v.cell::bigint % {workers_num} = e.worker_id
GROUP BY e.worker_id
ORDER BY e.worker_id
