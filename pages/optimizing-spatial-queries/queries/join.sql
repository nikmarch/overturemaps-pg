SELECT COUNT(*) FROM places p
JOIN divisions d ON p.geometry && d.geometry AND ST_Covers(d.geometry, p.geometry)
WHERE d.id = '{division_id}';
