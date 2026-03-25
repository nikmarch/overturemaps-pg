-- description: Four validations. 1) No cell is an ancestor of another (overlap check). 2) No places lost or double-counted (total count match). 3) No cell exceeds threshold (except res 10 fallback). 4) Boundary mismatch proof — finds places assigned by h3_latlng_to_cell but missed by ST_Intersects on the cell boundary polygon.
-- columns: overlapping_cells, total_count_check, threshold_violations, boundary_mismatches

-- 1. Overlap check: no emitted cell should be an ancestor of another emitted cell
SELECT count(*) AS overlapping_pairs
FROM (
  SELECT c.cell, h3_cell_to_parent(c.cell, gs) AS parent_cell
  FROM places_h3_t{threshold} c
  CROSS JOIN generate_series(1, c.res - 1) AS gs
) parents
JOIN places_h3_t{threshold} mv ON mv.cell = parents.parent_cell;

-- 2. Total count: sum of place_count in the view must equal total rows in places
SELECT
  (SELECT sum(place_count) FROM places_h3_t{threshold}) AS view_total,
  (SELECT count(*) FROM places) AS table_total,
  (SELECT sum(place_count) FROM places_h3_t{threshold}) = (SELECT count(*) FROM places) AS counts_match;

-- 3. Threshold violations: cells exceeding the threshold (res 10 fallback is expected)
SELECT cell, res, place_count
FROM places_h3_t{threshold}
WHERE place_count > {threshold}
ORDER BY place_count DESC
LIMIT 10;

-- 4. Boundary mismatch: places correctly assigned by index but outside the cell boundary polygon
SELECT c.cell, c.res, c.place_count,
  h3_cell_to_lat_lng(c.cell) AS cell_center,
  p.geometry::text AS place_geom
FROM places_h3_t{threshold} c
JOIN places p
  ON h3_latlng_to_cell(p.geometry::point, c.res) = c.cell
WHERE NOT ST_Intersects(p.geometry::geometry, h3_cell_to_boundary_geometry(c.cell))
LIMIT 5
