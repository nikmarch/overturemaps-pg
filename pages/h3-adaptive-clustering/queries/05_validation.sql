-- description: Validates that places assigned to a cell by H3 index at that resolution actually fall within the cell boundary. mismatched_places should be 0.
-- columns: mismatched_places
SELECT count(*) AS mismatched_places
FROM places_h3_t{threshold} c
JOIN places p
  ON h3_latlng_to_cell(p.geometry::point, c.res) = c.cell
WHERE NOT ST_Intersects(p.geometry::geometry, h3_cell_to_boundary_geometry(c.cell))
