-- error: quantified comparison (ANY)
SELECT id FROM orders WHERE id = ANY (SELECT id FROM customers)
