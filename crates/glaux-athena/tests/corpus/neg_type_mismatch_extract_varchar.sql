-- error: Unexpected parameters (varchar) for date/time function
SELECT EXTRACT(MONTH FROM note) FROM orders
