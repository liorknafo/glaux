-- Chained CTEs; the second references the first.
WITH shipped AS (
  SELECT customer_id, amount FROM orders WHERE status = 'shipped'
),
totals AS (
  SELECT customer_id, sum(amount) AS total, count(*) AS n FROM shipped GROUP BY customer_id
)
SELECT c.name, t.total, t.n
FROM totals t
JOIN customers c ON c.id = t.customer_id
ORDER BY t.total DESC
