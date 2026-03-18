-- columns: top_dense, area_coverage
SELECT cell, res, place_count,
  h3_cell_to_lat_lng(cell) AS center
FROM places_h3_t{threshold}
ORDER BY place_count DESC LIMIT 20;

SELECT res,
  count(*) AS cells,
  sum(place_count) AS places,
  round(sum(h3_cell_area(cell, 'km^2'))::numeric) AS total_area_km2,
  round((100.0 * sum(h3_cell_area(cell, 'km^2')) / 510e6)::numeric, 2) AS pct_earth
FROM places_h3_t{threshold}
GROUP BY res ORDER BY res
