-- Experimental: H3 materialized views over the `places` table
-- Requires extensions: postgis, h3
--
-- Run:
--   docker compose exec -T db psql -U postgres -d overturemaps -f /app/experiments/h3_places_materialized_views.sql

BEGIN;

-- 1) Per-place derived H3 cells (one column per resolution)
DROP MATERIALIZED VIEW IF EXISTS places_h3 CASCADE;
CREATE MATERIALIZED VIEW places_h3 AS
SELECT
  p.id,
  p.name,
  p.geometry,
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 1)  AS h3_r1,
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 2)  AS h3_r2,
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 3)  AS h3_r3,
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 4)  AS h3_r4,
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 5)  AS h3_r5,
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 6)  AS h3_r6,
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 7)  AS h3_r7,
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 8)  AS h3_r8,
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 9)  AS h3_r9,
  h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 10) AS h3_r10
FROM places p;

CREATE INDEX places_h3_h3_r10_idx ON places_h3 (h3_r10);

-- 2) Aggregated counts per H3 cell for resolutions 1..10
--    (handy for heatmaps / tiles / dashboards)
DROP MATERIALIZED VIEW IF EXISTS places_h3_counts CASCADE;
CREATE MATERIALIZED VIEW places_h3_counts AS
SELECT resolution, h3_cell, COUNT(*)::bigint AS place_count
FROM (
  SELECT 1  AS resolution, h3_r1  AS h3_cell FROM places_h3
  UNION ALL
  SELECT 2  AS resolution, h3_r2  AS h3_cell FROM places_h3
  UNION ALL
  SELECT 3  AS resolution, h3_r3  AS h3_cell FROM places_h3
  UNION ALL
  SELECT 4  AS resolution, h3_r4  AS h3_cell FROM places_h3
  UNION ALL
  SELECT 5  AS resolution, h3_r5  AS h3_cell FROM places_h3
  UNION ALL
  SELECT 6  AS resolution, h3_r6  AS h3_cell FROM places_h3
  UNION ALL
  SELECT 7  AS resolution, h3_r7  AS h3_cell FROM places_h3
  UNION ALL
  SELECT 8  AS resolution, h3_r8  AS h3_cell FROM places_h3
  UNION ALL
  SELECT 9  AS resolution, h3_r9  AS h3_cell FROM places_h3
  UNION ALL
  SELECT 10 AS resolution, h3_r10 AS h3_cell FROM places_h3
) t
GROUP BY resolution, h3_cell;

CREATE INDEX places_h3_counts_resolution_cell_idx ON places_h3_counts (resolution, h3_cell);
CREATE INDEX places_h3_counts_place_count_idx ON places_h3_counts (place_count DESC);

COMMIT;
