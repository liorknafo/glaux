-- error: approx_percentile
SELECT approx_percentile(amount, ARRAY[0.5, 0.9]) FROM orders
