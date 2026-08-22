-- error: not a valid DATE field
SELECT date_trunc('hour', signup_date) FROM customers
