-- h3_adaptive_cells_validation.sql
--
-- Validate that each place point lies within the H3 cell polygon for its claimed H3 index.
--
-- Context:
--   We generate adaptive-resolution H3 processing blocks in overturemaps-pg.
--   This query is an integrity check: points should be covered by the boundary
--   of the H3 cell index we assign them.
--
-- IMPORTANT:
--   Function names and column names may vary depending on how you store H3 in your schema.
--   Treat this as a "continue experiment" query: adjust identifiers, then run.
--
-- Recommended behavior:
--   Use ST_Covers(cell_poly, point) (NOT ST_Contains) so boundary points are not flagged.
--   Keep geometries in EPSG:4326 when using H3 boundary functions.
--
-- Assumptions (edit as needed):
--   places table:      places
--   point geometry:    geometry   (PostGIS geometry(Point,4326))
--   place id column:   id
--   place h3 column:   h3_index   (H3 index at the SAME resolution as used by your MV)
--   adaptive MV:       places_h3_adaptive_cells (or similar)
--   MV h3 column:      h3_index
--
-- If you do NOT store h3_index on places:
--   compute it from the point (ideally at a single high res, then parent),
--   or join via whatever mapping table you use.

-- Variant B (as discussed): MV provides the h3_index, we derive the polygon via H3.
-- Flags places whose point is NOT covered by the boundary polygon of its H3 index.
SELECT
  p.id,
  p.h3_index
FROM places p
JOIN places_h3_adaptive_cells c
  ON c.h3_index = p.h3_index
WHERE NOT ST_Covers(
  h3_cell_to_boundary(c.h3_index)::geometry,
  p.geometry
);

-- Debug helper: sample failing rows and print WKT
-- SELECT
--   p.id,
--   p.h3_index,
--   ST_AsText(p.geometry) AS place_wkt,
--   ST_AsText(h3_cell_to_boundary(c.h3_index)::geometry) AS cell_wkt
-- FROM places p
-- JOIN places_h3_adaptive_cells c ON c.h3_index = p.h3_index
-- WHERE NOT ST_Covers(h3_cell_to_boundary(c.h3_index)::geometry, p.geometry)
-- LIMIT 50;
