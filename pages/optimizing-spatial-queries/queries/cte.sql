WITH div AS MATERIALIZED (
    SELECT geometry as geom FROM divisions WHERE id = '{division_id}'
)
SELECT COUNT(*) FROM places p, div
WHERE p.geometry && div.geom
  AND ST_Covers(div.geom, p.geometry);
