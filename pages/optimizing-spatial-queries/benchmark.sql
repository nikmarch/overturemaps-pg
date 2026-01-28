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

\echo 'baseline EXPLAIN ANALYZE'
EXPLAIN ANALYZE
WITH div AS MATERIALIZED (
    SELECT geometry as geom FROM divisions WHERE id = '6aaadea0-9c48-4af0-a47f-bbe020540580'
)
SELECT * FROM places p, div
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

\echo 'simplified EXPLAIN ANALYZE'
EXPLAIN ANALYZE
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geometry, 0.01), 0.01) as geom
    FROM divisions WHERE id = '6aaadea0-9c48-4af0-a47f-bbe020540580'
)
SELECT * FROM places p, div
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

\echo 'baseline EXPLAIN ANALYZE'
EXPLAIN ANALYZE
WITH div AS MATERIALIZED (
    SELECT geometry as geom FROM divisions WHERE id = 'd95d3f8a-a2f4-4436-b0e4-a3a86d15008e'
)
SELECT * FROM places p, div
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

\echo 'simplified EXPLAIN ANALYZE'
EXPLAIN ANALYZE
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geometry, 0.01), 0.01) as geom
    FROM divisions WHERE id = 'd95d3f8a-a2f4-4436-b0e4-a3a86d15008e'
)
SELECT * FROM places p, div
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

\echo 'baseline EXPLAIN ANALYZE'
EXPLAIN ANALYZE
WITH div AS MATERIALIZED (
    SELECT geometry as geom FROM divisions WHERE id = '50a03a2c-3e24-4740-b80d-f933ea60c64f'
)
SELECT * FROM places p, div
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

\echo 'simplified EXPLAIN ANALYZE'
EXPLAIN ANALYZE
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geometry, 0.01), 0.01) as geom
    FROM divisions WHERE id = '50a03a2c-3e24-4740-b80d-f933ea60c64f'
)
SELECT * FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);
