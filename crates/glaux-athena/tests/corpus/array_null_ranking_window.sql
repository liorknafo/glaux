-- error: ARRAY comparison not supported for arrays with null elements
SELECT max(tags) OVER () FROM customers
