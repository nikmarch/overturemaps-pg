EXPLAIN ANALYZE
WITH div AS MATERIALIZED (
    SELECT ST_SIMPLIFYPRESERVETOPOLOGY(geometry, 0.01) AS geom
    FROM divisions
    WHERE id = '{id}'
)

SELECT * FROM places AS p, div
WHERE
    p.geometry && div.geom
    AND ST_COVERS(div.geom, p.geometry);

WITH div AS MATERIALIZED (
    SELECT ST_SIMPLIFYPRESERVETOPOLOGY(geometry, 0.01) AS geom
    FROM divisions
    WHERE id = '{id}'
)

SELECT COUNT(*) FROM places AS p, div
WHERE
    p.geometry && div.geom
    AND ST_COVERS(div.geom, p.geometry);
