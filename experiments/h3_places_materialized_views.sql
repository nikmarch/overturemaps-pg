-- H3 adaptive-resolution materialized view over `places`
-- Requires extensions: postgis, h3
--
-- Pre-computes h3 cells at resolutions 1-10 for every place, then builds
-- an adaptive frontier: coarse cells where count <= threshold, drilling
-- into finer resolutions only where needed. The result is a single
-- materialized view you can query as:
--
--   SELECT cell, res, place_count FROM places_h3_adaptive;
--
-- Run:
--   docker compose exec -T db psql -U postgres -d overturemaps \
--     -v threshold=10000 \
--     < experiments/h3_places_materialized_views.sql

DROP MATERIALIZED VIEW IF EXISTS places_h3_adaptive;

CREATE MATERIALIZED VIEW places_h3_adaptive AS
WITH
base AS (
  SELECT
    h3_latlng_to_cell(p.geometry::point, 1)  AS h3_r1,
    h3_latlng_to_cell(p.geometry::point, 2)  AS h3_r2,
    h3_latlng_to_cell(p.geometry::point, 3)  AS h3_r3,
    h3_latlng_to_cell(p.geometry::point, 4)  AS h3_r4,
    h3_latlng_to_cell(p.geometry::point, 5)  AS h3_r5,
    h3_latlng_to_cell(p.geometry::point, 6)  AS h3_r6,
    h3_latlng_to_cell(p.geometry::point, 7)  AS h3_r7,
    h3_latlng_to_cell(p.geometry::point, 8)  AS h3_r8,
    h3_latlng_to_cell(p.geometry::point, 9)  AS h3_r9,
    h3_latlng_to_cell(p.geometry::point, 10) AS h3_r10,
    COUNT(*)::bigint AS place_count
  FROM places p
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
r1 AS (
  SELECT 1 AS res, h3_r1 AS cell, SUM(place_count)::bigint AS cnt
  FROM base
  GROUP BY h3_r1
),
r2 AS (
  SELECT 2 AS res, b.h3_r2 AS cell, SUM(b.place_count)::bigint AS cnt
  FROM base b
  JOIN r1 ON b.h3_r1 = r1.cell AND r1.cnt > :threshold
  GROUP BY b.h3_r2
),
r3 AS (
  SELECT 3 AS res, b.h3_r3 AS cell, SUM(b.place_count)::bigint AS cnt
  FROM base b
  JOIN r2 ON b.h3_r2 = r2.cell AND r2.cnt > :threshold
  GROUP BY b.h3_r3
),
r4 AS (
  SELECT 4 AS res, b.h3_r4 AS cell, SUM(b.place_count)::bigint AS cnt
  FROM base b
  JOIN r3 ON b.h3_r3 = r3.cell AND r3.cnt > :threshold
  GROUP BY b.h3_r4
),
r5 AS (
  SELECT 5 AS res, b.h3_r5 AS cell, SUM(b.place_count)::bigint AS cnt
  FROM base b
  JOIN r4 ON b.h3_r4 = r4.cell AND r4.cnt > :threshold
  GROUP BY b.h3_r5
),
r6 AS (
  SELECT 6 AS res, b.h3_r6 AS cell, SUM(b.place_count)::bigint AS cnt
  FROM base b
  JOIN r5 ON b.h3_r5 = r5.cell AND r5.cnt > :threshold
  GROUP BY b.h3_r6
),
r7 AS (
  SELECT 7 AS res, b.h3_r7 AS cell, SUM(b.place_count)::bigint AS cnt
  FROM base b
  JOIN r6 ON b.h3_r6 = r6.cell AND r6.cnt > :threshold
  GROUP BY b.h3_r7
),
r8 AS (
  SELECT 8 AS res, b.h3_r8 AS cell, SUM(b.place_count)::bigint AS cnt
  FROM base b
  JOIN r7 ON b.h3_r7 = r7.cell AND r7.cnt > :threshold
  GROUP BY b.h3_r8
),
r9 AS (
  SELECT 9 AS res, b.h3_r9 AS cell, SUM(b.place_count)::bigint AS cnt
  FROM base b
  JOIN r8 ON b.h3_r8 = r8.cell AND r8.cnt > :threshold
  GROUP BY b.h3_r9
),
r10 AS (
  SELECT 10 AS res, b.h3_r10 AS cell, SUM(b.place_count)::bigint AS cnt
  FROM base b
  JOIN r9 ON b.h3_r9 = r9.cell AND r9.cnt > :threshold
  GROUP BY b.h3_r10
)
SELECT cell, res, cnt AS place_count FROM r1  WHERE cnt <= :threshold
UNION ALL
SELECT cell, res, cnt FROM r2  WHERE cnt <= :threshold
UNION ALL
SELECT cell, res, cnt FROM r3  WHERE cnt <= :threshold
UNION ALL
SELECT cell, res, cnt FROM r4  WHERE cnt <= :threshold
UNION ALL
SELECT cell, res, cnt FROM r5  WHERE cnt <= :threshold
UNION ALL
SELECT cell, res, cnt FROM r6  WHERE cnt <= :threshold
UNION ALL
SELECT cell, res, cnt FROM r7  WHERE cnt <= :threshold
UNION ALL
SELECT cell, res, cnt FROM r8  WHERE cnt <= :threshold
UNION ALL
SELECT cell, res, cnt FROM r9  WHERE cnt <= :threshold
UNION ALL
SELECT cell, res, cnt FROM r10;

CREATE INDEX places_h3_adaptive_cell_idx ON places_h3_adaptive (cell);
CREATE INDEX places_h3_adaptive_res_idx ON places_h3_adaptive (res);
