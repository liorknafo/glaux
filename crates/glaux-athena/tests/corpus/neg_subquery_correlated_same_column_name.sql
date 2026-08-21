-- error: correlated subquery over two `id` columns
-- DataFusion re-qualifies correlated columns onto the decorrelated join's
-- alias by their bare name, so `c.id` and `x.id` collapse onto one join key
-- and every row came back wrong (this query answered NULL / 0 everywhere).
-- Trino runs it, so refusing by name is the only acceptable outcome.
SELECT o.id,
       (SELECT c.name
        FROM customers c JOIN orders x ON x.customer_id = c.id
        WHERE c.id = o.customer_id AND x.id = o.id) AS n
FROM orders o
