\timing on

-- Germany
\echo '=== r51477 (Germany) ==='

\echo 'join'
SELECT COUNT(*) FROM places p
JOIN divisions d ON ST_Covers(d.geography, p.geography)
WHERE d.osm_id = 'r51477';

\echo 'cte'
WITH div AS MATERIALIZED (SELECT geography as geog FROM divisions WHERE osm_id = 'r51477')
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);

\echo 'cte_simplified'
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geography::geometry, 1000/111342.0), 1000/111342.0)::geography as geog
    FROM divisions WHERE osm_id = 'r51477'
)
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);

-- Chile
\echo '=== r167454 (Chile) ==='

\echo 'join'
SELECT COUNT(*) FROM places p
JOIN divisions d ON ST_Covers(d.geography, p.geography)
WHERE d.osm_id = 'r167454';

\echo 'cte'
WITH div AS MATERIALIZED (SELECT geography as geog FROM divisions WHERE osm_id = 'r167454')
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);

\echo 'cte_simplified'
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geography::geometry, 1000/111342.0), 1000/111342.0)::geography as geog
    FROM divisions WHERE osm_id = 'r167454'
)
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);

-- Russia
\echo '=== r60189 (Russia) ==='

\echo 'join'
SELECT COUNT(*) FROM places p
JOIN divisions d ON ST_Covers(d.geography, p.geography)
WHERE d.osm_id = 'r60189';

\echo 'cte'
WITH div AS MATERIALIZED (SELECT geography as geog FROM divisions WHERE osm_id = 'r60189')
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);

\echo 'cte_simplified'
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geography::geometry, 1000/111342.0), 1000/111342.0)::geography as geog
    FROM divisions WHERE osm_id = 'r60189'
)
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);
