-- Experimental: H3 materialized view over the `places` table
-- Requires extensions: postgis, h3
--
-- Run:
--   docker compose exec -T db psql -U postgres -d overturemaps -f /app/experiments/h3_places_materialized_views.sql

BEGIN;

-- One materialized view:
--   resolution (1..10), h3_cell, place_count
-- Computed directly from places.geometry.
DROP MATERIALIZED VIEW IF EXISTS places_h3_counts;
CREATE MATERIALIZED VIEW places_h3_counts AS
SELECT resolution, h3_cell, COUNT(*)::bigint AS place_count
FROM (
  SELECT 1  AS resolution, h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 1)  AS h3_cell FROM places p
  UNION ALL
  SELECT 2  AS resolution, h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 2)  AS h3_cell FROM places p
  UNION ALL
  SELECT 3  AS resolution, h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 3)  AS h3_cell FROM places p
  UNION ALL
  SELECT 4  AS resolution, h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 4)  AS h3_cell FROM places p
  UNION ALL
  SELECT 5  AS resolution, h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 5)  AS h3_cell FROM places p
  UNION ALL
  SELECT 6  AS resolution, h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 6)  AS h3_cell FROM places p
  UNION ALL
  SELECT 7  AS resolution, h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 7)  AS h3_cell FROM places p
  UNION ALL
  SELECT 8  AS resolution, h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 8)  AS h3_cell FROM places p
  UNION ALL
  SELECT 9  AS resolution, h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 9)  AS h3_cell FROM places p
  UNION ALL
  SELECT 10 AS resolution, h3_lat_lng_to_cell(ST_Y(p.geometry), ST_X(p.geometry), 10) AS h3_cell FROM places p
) t
GROUP BY resolution, h3_cell;

CREATE INDEX places_h3_counts_resolution_cell_idx ON places_h3_counts (resolution, h3_cell);
CREATE INDEX places_h3_counts_place_count_idx ON places_h3_counts (place_count DESC);

COMMIT;
