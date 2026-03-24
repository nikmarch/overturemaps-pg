-- description: Spatial overlap validation. Converts cells to polygons and checks that no cell contains another. overlapping_pairs should be 0.
-- columns: overlapping_cells
WITH cells AS (
  SELECT cell, res, place_count,
    h3_cell_to_boundary_geometry(cell) AS geom
  FROM places_h3_t{threshold}
)
SELECT count(*) AS overlapping_pairs
FROM cells a
JOIN cells b ON a.res < b.res
  AND ST_Contains(a.geom, b.geom)
