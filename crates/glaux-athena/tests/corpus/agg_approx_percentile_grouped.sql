-- approx_percentile per group, including a group whose only amount is NULL.
SELECT status, approx_percentile(amount, 0.5) AS median, count(amount) AS n
FROM orders
GROUP BY status
ORDER BY status
