-- error: All NULLIF operands must be the same type
SELECT nullif(id, '1') FROM customers
