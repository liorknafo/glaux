-- ORDER BY with explicit NULL placement, LIMIT and OFFSET.
SELECT id, amount
FROM orders
ORDER BY amount DESC NULLS LAST, id
LIMIT 3 OFFSET 2
