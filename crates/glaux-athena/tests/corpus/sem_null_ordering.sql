-- Trino sorts NULLs last whatever the direction (DataFusion alone puts them first under DESC): top-level, window, and aggregate ORDER BY without an explicit NULLS clause.
SELECT id, amount,
       rank() OVER (ORDER BY amount DESC) AS rnk,
       row_number() OVER (ORDER BY amount DESC) AS rn,
       first_value(id) OVER (ORDER BY amount DESC) AS top_id,
       array_agg(id) OVER (ORDER BY amount DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ids_so_far
FROM orders
ORDER BY amount DESC, id
LIMIT 4
