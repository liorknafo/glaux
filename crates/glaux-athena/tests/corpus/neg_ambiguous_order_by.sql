-- error: Column 'x' is ambiguous
SELECT id x, amount x FROM orders ORDER BY x LIMIT 1
