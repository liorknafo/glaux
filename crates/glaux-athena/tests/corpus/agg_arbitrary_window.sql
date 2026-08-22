-- `arbitrary` as a window aggregate: the frame for order 106 is (80.25,
-- NULL) and answers 80.25, not NULL.
SELECT id, arbitrary(amount) OVER (PARTITION BY status ORDER BY id) AS any_amount
FROM orders
WHERE status = 'pending'
ORDER BY id
