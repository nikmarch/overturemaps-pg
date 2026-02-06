-- columns: analyze, total
EXPLAIN ANALYZE
SELECT * FROM places AS p
INNER JOIN
    divisions AS d
    ON p.geometry && d.geometry AND ST_COVERS(d.geometry, p.geometry)
WHERE d.id = '{id}';

SELECT COUNT(*) FROM places AS p
INNER JOIN
    divisions AS d
    ON p.geometry && d.geometry AND ST_COVERS(d.geometry, p.geometry)
WHERE d.id = '{id}';
