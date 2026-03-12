-- Experimental: H3 materialized view over the `places` table
-- Requires extensions: postgis, h3
--
-- Run:
--   docker compose exec -T db psql -U postgres -d overturemaps -f /app/experiments/h3_places_materialized_views.sql

BEGIN;

-- Materialized view: counts by smallest cells (r10)
--   h3_r10, place_count
DROP MATERIALIZED VIEW IF EXISTS places_h3_r10_counts;
CREATE MATERIALIZED VIEW places_h3_r10_counts AS
SELECT
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 10) AS h3_r10,
  COUNT(*)::bigint AS place_count
FROM places p
GROUP BY 1;

CREATE INDEX places_h3_r10_counts_cell_idx ON places_h3_r10_counts (h3_r10);
CREATE INDEX places_h3_r10_counts_place_count_idx ON places_h3_r10_counts (place_count DESC);

-- Optional: one-query adaptive rollup to coarser parents until bucket size is <= threshold (or r1).
-- This produces a set of (res, cell, place_count) where:
--  - place_count <= threshold
--  - but the parent cell's count is > threshold (so this is the coarsest acceptable level)
--
-- Usage:
--   psql ... -v threshold=10000 -f /app/experiments/h3_places_materialized_views.sql
--
-- NOTE: h3-pg function names vary. We create a small wrapper that calls whichever exists.
CREATE OR REPLACE FUNCTION h3_parent(cell bigint, parent_resolution int)
RETURNS bigint
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  IF to_regprocedure('h3_cell_to_parent(bigint,integer)') IS NOT NULL THEN
    RETURN h3_cell_to_parent(cell, parent_resolution);
  ELSIF to_regprocedure('h3_to_parent(bigint,integer)') IS NOT NULL THEN
    RETURN h3_to_parent(cell, parent_resolution);
  ELSE
    RAISE EXCEPTION 'No H3 parent function found (expected h3_cell_to_parent or h3_to_parent).';
  END IF;
END;
$$;

-- Query #1: simple rollup
-- "How many places are inside each H3 cell at resolution N?"
--
-- Example (set N=9):
--
--   SELECT
--     h3_parent(h3_r10, 9) AS h3_r9,
--     SUM(place_count)::bigint AS place_count
--   FROM places_h3_r10_counts
--   GROUP BY 1
--   ORDER BY place_count DESC;
--
-- Query #2: adaptive frontier (mixed resolutions)
-- Returns a set of (res, cell, place_count) such that each returned cell:
--   - has place_count <= :threshold
--   - but its parent would exceed :threshold (so this is the coarsest safe bucket)
--   - or it is already at res=1
--
-- WITH RECURSIVE counts AS (
--   SELECT 10 AS res, h3_r10 AS cell, place_count AS cnt
--   FROM places_h3_r10_counts
--
--   UNION ALL
--
--   SELECT c.res - 1 AS res, h3_parent(c.cell, c.res - 1) AS cell, SUM(c.cnt)::bigint AS cnt
--   FROM counts c
--   WHERE c.res > 1
--   GROUP BY 1,2
-- ), with_parent AS (
--   SELECT
--     c.res,
--     c.cell,
--     c.cnt,
--     p.cnt AS parent_cnt
--   FROM counts c
--   LEFT JOIN counts p
--     ON p.res = c.res - 1
--    AND p.cell = h3_parent(c.cell, c.res - 1)
-- )
-- SELECT res, cell, cnt AS place_count
-- FROM with_parent
-- WHERE cnt <= :threshold
--   AND (res = 1 OR parent_cnt > :threshold)
-- ORDER BY res, place_count DESC;

COMMIT;
