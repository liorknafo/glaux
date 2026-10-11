-- ROLLUP produces subtotal and grand-total rows.
SELECT c.country, o.status, sum(o.amount) AS total
FROM orders o
JOIN customers c ON c.id = o.customer_id
WHERE o.amount IS NOT NULL
GROUP BY ROLLUP (c.country, o.status)
ORDER BY c.country NULLS LAST, o.status NULLS LAST
