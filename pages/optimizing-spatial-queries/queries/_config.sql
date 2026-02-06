SELECT
    id,
    osm_id,
    class,
    name,
    ST_NPOINTS(geometry) AS points,
    ST_AREA(geometry) AS sqm
FROM divisions
WHERE
    osm_id LIKE 'r%'
    AND ST_NPOINTS(geometry) > 1000
ORDER BY ST_AREA(geometry) DESC
LIMIT 200;
