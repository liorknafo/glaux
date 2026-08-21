-- error: 'z'
SELECT format_datetime(created_at, 'yyyy z') FROM orders
