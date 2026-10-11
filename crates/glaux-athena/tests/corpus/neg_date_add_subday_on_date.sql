-- error: cannot be added to a DATE
SELECT date_add('hour', 1, signup_date) FROM customers
