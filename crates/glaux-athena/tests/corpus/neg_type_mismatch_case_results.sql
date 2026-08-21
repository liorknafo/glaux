-- error: All CASE results must be the same type
SELECT CASE WHEN id = 1 THEN 1 ELSE 'a' END FROM customers
