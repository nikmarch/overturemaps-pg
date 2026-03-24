-- description: Distribution stats. Resolution breakdown (cells, places, avg/min/max per cell), decile distribution via ntile(10), and totals with percentile array.
-- columns: by_resolution, deciles, totals
SELECT res, count(*) AS cells, sum(place_count) AS places,
  round(avg(place_count)) AS avg_per_cell,
  min(place_count) AS min_per_cell,
  max(place_count) AS max_per_cell
FROM places_h3_t{threshold} GROUP BY res ORDER BY res;

SELECT
  decile,
  count(*) AS cells,
  min(place_count) AS min_count,
  round(avg(place_count)) AS avg_count,
  max(place_count) AS max_count,
  sum(place_count) AS total_places
FROM (
  SELECT place_count, ntile(10) OVER (ORDER BY place_count) AS decile
  FROM places_h3_t{threshold}
) d
GROUP BY decile ORDER BY decile;

SELECT
  count(*) AS total_cells,
  sum(place_count) AS total_places,
  round(avg(place_count)) AS avg,
  round(stddev(place_count)) AS stddev,
  min(place_count) AS min,
  max(place_count) AS max,
  count(*) FILTER (WHERE place_count = 1) AS single_place_cells
FROM places_h3_t{threshold}
