-- error: Unexpected parameters (varchar) for date/time function
SELECT date_format('2024-01-05 10:00:00', '%Y') FROM orders
