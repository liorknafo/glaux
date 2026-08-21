-- error: Cannot apply operator: bigint = varchar
SELECT * FROM customers c JOIN orders o ON c.id = o.status
