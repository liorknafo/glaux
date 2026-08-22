-- UNION ALL / UNION / INTERSECT / EXCEPT.
SELECT x FROM (
  SELECT customer_id AS x FROM orders WHERE status = 'shipped'
  UNION ALL
  SELECT id FROM customers WHERE country = 'DE'
  UNION
  SELECT 42
  INTERSECT
  SELECT id FROM customers
  EXCEPT
  SELECT 1
) t
ORDER BY x
