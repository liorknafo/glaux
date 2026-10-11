-- Aliases resolve case-insensitively and are reported as written (quoted) or lower-cased (unquoted), as in Trino; ORDER BY may repeat a mixed-case quoted alias in any case.
SELECT c.name, sum(o.amount) AS "Total", count(*) AS NumOrders
FROM orders o
JOIN customers c ON c.id = o.customer_id
GROUP BY c.name
ORDER BY total DESC, NUMORDERS
