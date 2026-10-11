-- error: quantified comparison (ALL)
SELECT id FROM orders WHERE id > ALL (SELECT id FROM customers)
