-- H3 adaptive-resolution materialized view over `places`
-- Requires extensions: postgis, h3
--
-- Pre-computes h3 cells at resolutions 1-10 for every place, then assigns
-- each place to the coarsest resolution where its cell count <= threshold.
-- Each place is counted exactly once, so output cells never overlap.
--
--   SELECT cell, res, place_count FROM places_h3_adaptive;
--
-- Run:
--   ./build_views.sh                # default threshold=10000
--   ./build_views.sh 5000           # custom threshold

DROP MATERIALIZED VIEW IF EXISTS places_h3_adaptive;

CREATE MATERIALIZED VIEW places_h3_adaptive AS
WITH
-- Compute r10 once, derive all coarser resolutions via h3_cell_to_parent
-- to guarantee strict nesting (children always sum to parent count)
base AS (
  SELECT
    h3_r10,
    h3_cell_to_parent(h3_r10, 9)  AS h3_r9,
    h3_cell_to_parent(h3_r10, 8)  AS h3_r8,
    h3_cell_to_parent(h3_r10, 7)  AS h3_r7,
    h3_cell_to_parent(h3_r10, 6)  AS h3_r6,
    h3_cell_to_parent(h3_r10, 5)  AS h3_r5,
    h3_cell_to_parent(h3_r10, 4)  AS h3_r4,
    h3_cell_to_parent(h3_r10, 3)  AS h3_r3,
    h3_cell_to_parent(h3_r10, 2)  AS h3_r2,
    h3_cell_to_parent(h3_r10, 1)  AS h3_r1,
    place_count
  FROM (
    SELECT
      h3_latlng_to_cell(p.geometry::point, 10) AS h3_r10,
      COUNT(*)::bigint AS place_count
    FROM places p
    GROUP BY 1
  ) r10
),
-- Count places per cell at each resolution
c1  AS (SELECT h3_r1  AS cell, SUM(place_count)::bigint AS cnt FROM base GROUP BY 1),
c2  AS (SELECT h3_r2  AS cell, SUM(place_count)::bigint AS cnt FROM base GROUP BY 1),
c3  AS (SELECT h3_r3  AS cell, SUM(place_count)::bigint AS cnt FROM base GROUP BY 1),
c4  AS (SELECT h3_r4  AS cell, SUM(place_count)::bigint AS cnt FROM base GROUP BY 1),
c5  AS (SELECT h3_r5  AS cell, SUM(place_count)::bigint AS cnt FROM base GROUP BY 1),
c6  AS (SELECT h3_r6  AS cell, SUM(place_count)::bigint AS cnt FROM base GROUP BY 1),
c7  AS (SELECT h3_r7  AS cell, SUM(place_count)::bigint AS cnt FROM base GROUP BY 1),
c8  AS (SELECT h3_r8  AS cell, SUM(place_count)::bigint AS cnt FROM base GROUP BY 1),
c9  AS (SELECT h3_r9  AS cell, SUM(place_count)::bigint AS cnt FROM base GROUP BY 1),
c10 AS (SELECT h3_r10 AS cell, SUM(place_count)::bigint AS cnt FROM base GROUP BY 1),
-- Tag each base row with the coarsest resolution where its cell fits under threshold
tagged AS (
  SELECT
    b.*,
    CASE
      WHEN c1.cnt  <= :threshold THEN 1
      WHEN c2.cnt  <= :threshold THEN 2
      WHEN c3.cnt  <= :threshold THEN 3
      WHEN c4.cnt  <= :threshold THEN 4
      WHEN c5.cnt  <= :threshold THEN 5
      WHEN c6.cnt  <= :threshold THEN 6
      WHEN c7.cnt  <= :threshold THEN 7
      WHEN c8.cnt  <= :threshold THEN 8
      WHEN c9.cnt  <= :threshold THEN 9
      ELSE 10
    END AS emit_res
  FROM base b
  JOIN c1  ON b.h3_r1  = c1.cell
  JOIN c2  ON b.h3_r2  = c2.cell
  JOIN c3  ON b.h3_r3  = c3.cell
  JOIN c4  ON b.h3_r4  = c4.cell
  JOIN c5  ON b.h3_r5  = c5.cell
  JOIN c6  ON b.h3_r6  = c6.cell
  JOIN c7  ON b.h3_r7  = c7.cell
  JOIN c8  ON b.h3_r8  = c8.cell
  JOIN c9  ON b.h3_r9  = c9.cell
  JOIN c10 ON b.h3_r10 = c10.cell
)
SELECT h3_r1 AS cell, 1 AS res, SUM(place_count)::bigint AS place_count
  FROM tagged WHERE emit_res = 1 GROUP BY h3_r1
UNION ALL
SELECT h3_r2, 2, SUM(place_count)::bigint
  FROM tagged WHERE emit_res = 2 GROUP BY h3_r2
UNION ALL
SELECT h3_r3, 3, SUM(place_count)::bigint
  FROM tagged WHERE emit_res = 3 GROUP BY h3_r3
UNION ALL
SELECT h3_r4, 4, SUM(place_count)::bigint
  FROM tagged WHERE emit_res = 4 GROUP BY h3_r4
UNION ALL
SELECT h3_r5, 5, SUM(place_count)::bigint
  FROM tagged WHERE emit_res = 5 GROUP BY h3_r5
UNION ALL
SELECT h3_r6, 6, SUM(place_count)::bigint
  FROM tagged WHERE emit_res = 6 GROUP BY h3_r6
UNION ALL
SELECT h3_r7, 7, SUM(place_count)::bigint
  FROM tagged WHERE emit_res = 7 GROUP BY h3_r7
UNION ALL
SELECT h3_r8, 8, SUM(place_count)::bigint
  FROM tagged WHERE emit_res = 8 GROUP BY h3_r8
UNION ALL
SELECT h3_r9, 9, SUM(place_count)::bigint
  FROM tagged WHERE emit_res = 9 GROUP BY h3_r9
UNION ALL
SELECT h3_r10, 10, SUM(place_count)::bigint
  FROM tagged WHERE emit_res = 10 GROUP BY h3_r10;

CREATE INDEX places_h3_adaptive_cell_idx ON places_h3_adaptive (cell);
CREATE INDEX places_h3_adaptive_res_idx ON places_h3_adaptive (res);
