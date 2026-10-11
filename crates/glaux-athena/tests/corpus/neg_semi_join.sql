-- error: SEMI / ANTI JOIN
SELECT * FROM customers c LEFT SEMI JOIN orders o ON o.customer_id = c.id
