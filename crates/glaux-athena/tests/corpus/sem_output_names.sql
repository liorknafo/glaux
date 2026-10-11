-- Anonymous output columns are _colN by position; duplicate output names are allowed.
SELECT count(*), id, 1 AS a, 2 AS A, id, name AS "Name", id + 1
FROM customers
WHERE id = 1
GROUP BY id, name
