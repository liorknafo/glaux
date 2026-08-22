-- LEFT JOIN with USING-free ON predicate; unmatched customers keep NULL order columns.
SELECT c.id, c.name, o.id AS order_id, o.amount
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'shipped'
WHERE c.country IN ('US', 'FR')
ORDER BY c.id, order_id
