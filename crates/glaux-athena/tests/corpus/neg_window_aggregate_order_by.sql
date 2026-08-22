-- array_agg(x ORDER BY y) OVER (...) is valid Trino; glaux's engine cannot order an aggregate's input inside a window frame, so it is refused by name instead of leaking DataFusion's "Aggregate ORDER BY is not implemented for window functions" as a syntax error.
-- error: aggregate ORDER BY inside a window function
SELECT array_agg(id ORDER BY id DESC) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM customers
