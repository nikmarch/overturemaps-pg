-- Spatial query benchmark
-- Each query is labeled with a comment: -- @query <name>
-- Variables use {division_id} placeholder

-- @query join
-- Direct JOIN without CTE
SELECT COUNT(*) FROM places p
JOIN divisions d ON p.geometry && d.geometry AND ST_Covers(d.geometry, p.geometry)
WHERE d.id = '{division_id}';

-- @query cte
-- Using MATERIALIZED CTE
WITH div AS MATERIALIZED (
    SELECT geometry as geom FROM divisions WHERE id = '{division_id}'
)
SELECT COUNT(*) FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);

-- @query simplified
-- Using MATERIALIZED CTE with ST_Simplify + ST_Buffer
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geometry, 0.01), 0.01) as geom
    FROM divisions WHERE id = '{division_id}'
)
SELECT COUNT(*) FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);
