-- Using MATERIALIZED CTE with ST_Simplify + ST_Buffer
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geometry, 0.01), 0.01) as geom
    FROM divisions WHERE id = '{id}'
)
SELECT COUNT(*) FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);
