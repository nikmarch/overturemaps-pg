-- description: Two validations. 1) No cell is an ancestor of another (overlap check). 2) Proves spatial intersection can lose data vs H3 index — finds a place correctly assigned by h3_latlng_to_cell but missed by ST_Intersects on the cell boundary.
-- columns: overlapping_cells, boundary_mismatch
SELECT count(*) AS overlapping_pairs
FROM (
  SELECT c.cell, h3_cell_to_parent(c.cell, gs) AS parent_cell
  FROM places_h3_t{threshold} c
  CROSS JOIN generate_series(1, c.res - 1) AS gs
) parents
JOIN places_h3_t{threshold} mv ON mv.cell = parents.parent_cell;

SELECT c.cell, c.res, c.place_count,
  h3_cell_to_lat_lng(c.cell) AS cell_center,
  p.geometry::text AS place_geom
FROM places_h3_t{threshold} c
JOIN places p
  ON h3_latlng_to_cell(p.geometry::point, c.res) = c.cell
WHERE NOT ST_Intersects(p.geometry::geometry, h3_cell_to_boundary_geometry(c.cell))
LIMIT 1
