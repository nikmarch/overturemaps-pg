\timing on

-- Argentina
\echo '=== Argentina (6aaadea0-9c48-4af0-a47f-bbe020540580) ==='

\echo 'join'
SELECT COUNT(*) FROM places p
JOIN divisions d ON ST_Covers(d.geography, p.geography)
WHERE d.id = '6aaadea0-9c48-4af0-a47f-bbe020540580';

\echo 'cte'
WITH div AS MATERIALIZED (SELECT geography as geog FROM divisions WHERE id = '6aaadea0-9c48-4af0-a47f-bbe020540580')
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);

\echo 'cte_simplified'
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geography::geometry, 1000/111342.0), 1000/111342.0)::geography as geog
    FROM divisions WHERE id = '6aaadea0-9c48-4af0-a47f-bbe020540580'
)
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);

-- Deutschland
\echo '=== Deutschland (d95d3f8a-a2f4-4436-b0e4-a3a86d15008e) ==='

\echo 'join'
SELECT COUNT(*) FROM places p
JOIN divisions d ON ST_Covers(d.geography, p.geography)
WHERE d.id = 'd95d3f8a-a2f4-4436-b0e4-a3a86d15008e';

\echo 'cte'
WITH div AS MATERIALIZED (SELECT geography as geog FROM divisions WHERE id = 'd95d3f8a-a2f4-4436-b0e4-a3a86d15008e')
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);

\echo 'cte_simplified'
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geography::geometry, 1000/111342.0), 1000/111342.0)::geography as geog
    FROM divisions WHERE id = 'd95d3f8a-a2f4-4436-b0e4-a3a86d15008e'
)
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);

-- United States
\echo '=== United States (50a03a2c-3e24-4740-b80d-f933ea60c64f) ==='

\echo 'join'
SELECT COUNT(*) FROM places p
JOIN divisions d ON ST_Covers(d.geography, p.geography)
WHERE d.id = '50a03a2c-3e24-4740-b80d-f933ea60c64f';

\echo 'cte'
WITH div AS MATERIALIZED (SELECT geography as geog FROM divisions WHERE id = '50a03a2c-3e24-4740-b80d-f933ea60c64f')
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);

\echo 'cte_simplified'
WITH div AS MATERIALIZED (
    SELECT ST_Buffer(ST_Simplify(geography::geometry, 1000/111342.0), 1000/111342.0)::geography as geog
    FROM divisions WHERE id = '50a03a2c-3e24-4740-b80d-f933ea60c64f'
)
SELECT COUNT(*) FROM places p, div WHERE ST_Covers(div.geog, p.geography);
