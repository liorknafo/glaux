-- error: approx_percentile
SELECT approx_percentile(CAST(amount AS DECIMAL(10, 2)), 0.5) FROM orders
