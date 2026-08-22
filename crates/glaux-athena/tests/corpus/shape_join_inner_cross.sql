-- INNER JOIN through a derived table plus a CROSS JOIN against a one-row subquery.
SELECT c.name, t.n_orders, g.grand_total
FROM customers c
INNER JOIN (SELECT customer_id, count(*) AS n_orders FROM orders GROUP BY customer_id) t
  ON t.customer_id = c.id
CROSS JOIN (SELECT sum(amount) AS grand_total FROM orders) g
ORDER BY c.name
