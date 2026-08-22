-- error: QUALIFY
SELECT id FROM orders QUALIFY row_number() OVER (ORDER BY id) = 1
