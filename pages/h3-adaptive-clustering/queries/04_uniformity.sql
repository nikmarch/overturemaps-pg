-- description: Gini coefficient measuring how evenly places are distributed across cells. 0 = perfectly uniform, 1 = all places in one cell.
-- columns: gini
SELECT
  round((1 - sum(cumulative_share) * 2.0 / count(*))::numeric, 4) AS gini
FROM (
  SELECT place_count,
    sum(place_count) OVER (ORDER BY place_count) * 1.0
      / sum(place_count) OVER () AS cumulative_share
  FROM places_h3_t{threshold}
) g
