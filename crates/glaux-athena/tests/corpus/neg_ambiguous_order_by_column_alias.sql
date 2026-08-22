-- error: Column 'status' is ambiguous
SELECT amount AS status, status FROM orders ORDER BY status
