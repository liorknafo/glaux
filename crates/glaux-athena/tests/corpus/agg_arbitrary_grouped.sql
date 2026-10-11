-- The same non-null contract per group and per window frame: the `pending`
-- group is (80.25, NULL) and the row for order 106 sees both.
SELECT status,
       arbitrary(amount) AS any_amount,
       arbitrary(rush) AS any_rush
FROM orders
GROUP BY status
ORDER BY status
