-- columns: overlapping_cells, parent_child_pairs
SELECT count(*) AS overlapping_pairs
FROM places_h3_t{threshold} a
JOIN places_h3_t{threshold} b
  ON a.res < b.res
  AND h3_cell_to_parent(b.cell, a.res) = a.cell;

SELECT a.cell AS parent_cell, a.res AS parent_res, a.place_count AS parent_count,
  b.cell AS child_cell, b.res AS child_res, b.place_count AS child_count
FROM places_h3_t{threshold} a
JOIN places_h3_t{threshold} b
  ON a.res < b.res
  AND h3_cell_to_parent(b.cell, a.res) = a.cell
LIMIT 20
