-- error: quantified comparison (SOME)
SELECT id FROM orders WHERE id >= SOME (SELECT id FROM customers)
