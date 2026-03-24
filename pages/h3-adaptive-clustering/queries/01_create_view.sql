-- description: Create adaptive H3 materialized view. Each place is assigned to the coarsest resolution where its cell count stays under the threshold. Strict hierarchy via h3_cell_to_parent.
-- columns: drop, create_view, count
DROP MATERIALIZED VIEW IF EXISTS places_h3_t{threshold};

CREATE MATERIALIZED VIEW places_h3_t{threshold} AS
WITH
base AS (
  SELECT h3_r10, place_count,
    h3_cell_to_parent(h3_r10, 9) AS h3_r9, h3_cell_to_parent(h3_r10, 8) AS h3_r8,
    h3_cell_to_parent(h3_r10, 7) AS h3_r7, h3_cell_to_parent(h3_r10, 6) AS h3_r6,
    h3_cell_to_parent(h3_r10, 5) AS h3_r5, h3_cell_to_parent(h3_r10, 4) AS h3_r4,
    h3_cell_to_parent(h3_r10, 3) AS h3_r3, h3_cell_to_parent(h3_r10, 2) AS h3_r2,
    h3_cell_to_parent(h3_r10, 1) AS h3_r1
  FROM (
    SELECT h3_latlng_to_cell(p.geometry::point, 10) AS h3_r10,
           COUNT(*)::bigint AS place_count
    FROM places p GROUP BY 1
  ) r10
),
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
tagged AS (
  SELECT b.h3_r10,
    CASE
      WHEN c1.cnt  <= {threshold} THEN 1 WHEN c2.cnt  <= {threshold} THEN 2
      WHEN c3.cnt  <= {threshold} THEN 3 WHEN c4.cnt  <= {threshold} THEN 4
      WHEN c5.cnt  <= {threshold} THEN 5 WHEN c6.cnt  <= {threshold} THEN 6
      WHEN c7.cnt  <= {threshold} THEN 7 WHEN c8.cnt  <= {threshold} THEN 8
      WHEN c9.cnt  <= {threshold} THEN 9 ELSE 10
    END AS emit_res
  FROM base b
  JOIN c1 ON b.h3_r1=c1.cell JOIN c2 ON b.h3_r2=c2.cell JOIN c3 ON b.h3_r3=c3.cell
  JOIN c4 ON b.h3_r4=c4.cell JOIN c5 ON b.h3_r5=c5.cell JOIN c6 ON b.h3_r6=c6.cell
  JOIN c7 ON b.h3_r7=c7.cell JOIN c8 ON b.h3_r8=c8.cell JOIN c9 ON b.h3_r9=c9.cell
  JOIN c10 ON b.h3_r10=c10.cell
)
SELECT
  h3_cell_to_parent(t.h3_r10, t.emit_res) AS cell,
  t.emit_res AS res,
  SUM(b.place_count)::bigint AS place_count
FROM base b
JOIN tagged t ON b.h3_r10 = t.h3_r10
GROUP BY 1, 2;

SELECT count(*) AS total_cells FROM places_h3_t{threshold}
