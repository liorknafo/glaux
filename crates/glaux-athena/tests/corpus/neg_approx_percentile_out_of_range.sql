-- error: Percentile must be between 0 and 1
SELECT approx_percentile(amount, 1.5) FROM orders
