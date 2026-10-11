-- error: value and result of subquery must be of the same type
SELECT id FROM customers WHERE id IN (SELECT status FROM orders)
