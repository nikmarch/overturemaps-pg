-- description: Overlap validation. Checks that no cell in the MV is an ancestor of another — overlapping_pairs should be 0 for a correct adaptive frontier.
-- columns: overlapping_cells
SELECT count(*) AS overlapping_pairs
FROM (
  SELECT c.cell, h3_cell_to_parent(c.cell, gs) AS parent_cell
  FROM places_h3_t{threshold} c
  CROSS JOIN generate_series(1, c.res - 1) AS gs
) parents
JOIN places_h3_t{threshold} mv ON mv.cell = parents.parent_cell
