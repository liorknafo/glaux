-- error: All COALESCE operands must be the same type
SELECT coalesce(amount, 'none') FROM orders
