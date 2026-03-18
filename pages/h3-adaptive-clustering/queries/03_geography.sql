-- columns: top_dense, area_coverage
SELECT cell, res, place_count,
  h3_cell_to_lat_lng(cell) AS center
FROM places_h3_t{threshold}
ORDER BY place_count DESC LIMIT 20;

SELECT res,
  count(*) AS cells,
  sum(place_count) AS places,
  round(count(*) * h3_cell_area(h3_latlng_to_cell('POINT(0 0)'::point, res), 'km^2')::numeric) AS approx_area_km2,
  round(100.0 * count(*) * h3_cell_area(h3_latlng_to_cell('POINT(0 0)'::point, res), 'km^2')
    / 510e6, 2) AS pct_earth
FROM places_h3_t{threshold}
GROUP BY res ORDER BY res
