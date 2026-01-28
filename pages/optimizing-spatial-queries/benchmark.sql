-- Ensure indexes exist
CREATE INDEX IF NOT EXISTS places_geometry_idx ON places USING GIST (geometry);
CREATE INDEX IF NOT EXISTS divisions_geometry_idx ON divisions USING GIST (geometry);
CREATE INDEX IF NOT EXISTS divisions_id_idx ON divisions (id);

-- Update statistics for query planner
ANALYZE places;
ANALYZE divisions;

-- Performance tuning (session-level)
SET work_mem = '1GB';
SET effective_cache_size = '12GB';
SET max_parallel_workers_per_gather = 4;
SET random_page_cost = 1.1;  -- for SSD
SET parallel_tuple_cost = 0.001;  -- encourage parallelism
SET parallel_setup_cost = 100;    -- lower barrier for parallel plans

\timing on

-- Argentina
\echo '=== Argentina (6aaadea0-9c48-4af0-a47f-bbe020540580) ==='

\echo 'baseline (geometry + bbox)'
WITH div AS MATERIALIZED (
    SELECT geometry as geom FROM divisions WHERE id = '6aaadea0-9c48-4af0-a47f-bbe020540580'
)
SELECT COUNT(*) FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);

\echo 'simplified (geometry + bbox + ST_Simplify)'
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geometry, 0.01), 0.01) as geom
    FROM divisions WHERE id = '6aaadea0-9c48-4af0-a47f-bbe020540580'
)
SELECT COUNT(*) FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);

-- Deutschland
\echo '=== Deutschland (d95d3f8a-a2f4-4436-b0e4-a3a86d15008e) ==='

\echo 'baseline (geometry + bbox)'
WITH div AS MATERIALIZED (
    SELECT geometry as geom FROM divisions WHERE id = 'd95d3f8a-a2f4-4436-b0e4-a3a86d15008e'
)
SELECT COUNT(*) FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);

\echo 'simplified (geometry + bbox + ST_Simplify)'
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geometry, 0.01), 0.01) as geom
    FROM divisions WHERE id = 'd95d3f8a-a2f4-4436-b0e4-a3a86d15008e'
)
SELECT COUNT(*) FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);

-- United States
\echo '=== United States (50a03a2c-3e24-4740-b80d-f933ea60c64f) ==='

\echo 'baseline (geometry + bbox)'
WITH div AS MATERIALIZED (
    SELECT geometry as geom FROM divisions WHERE id = '50a03a2c-3e24-4740-b80d-f933ea60c64f'
)
SELECT COUNT(*) FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);

\echo 'simplified (geometry + bbox + ST_Simplify)'
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geometry, 0.01), 0.01) as geom
    FROM divisions WHERE id = '50a03a2c-3e24-4740-b80d-f933ea60c64f'
)
SELECT COUNT(*) FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);
