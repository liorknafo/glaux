-- error: All GREATEST operands must be the same type
SELECT greatest(id, 'a') FROM customers
