-- The same shape as a WHERE predicate.
SELECT o.id
FROM orders o
WHERE (SELECT c.country FROM customers c WHERE c.id = o.customer_id) = 'US'
ORDER BY o.id
