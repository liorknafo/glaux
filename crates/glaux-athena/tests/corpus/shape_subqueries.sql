-- Correlated EXISTS, IN (subquery), and a scalar subquery in the projection.
SELECT c.id, c.name,
       (SELECT max(o.amount) FROM orders o WHERE o.customer_id = c.id) AS max_amount
FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.status = 'shipped')
  AND c.id IN (SELECT customer_id FROM orders WHERE amount > 30)
ORDER BY c.id
