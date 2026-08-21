-- Three formats in one query: Parquet customers, JSON orders, CSV countries, with a CTE and a window.
WITH spend AS (
    SELECT customer_id, sum(amount) AS total, count(*) AS n
    FROM orders
    WHERE amount IS NOT NULL
    GROUP BY customer_id
)
SELECT c.name, co.name AS country, co.continent, s.total, s.n,
       rank() OVER (PARTITION BY co.continent ORDER BY s.total DESC) AS rank_in_continent
FROM customers c
JOIN countries co ON co.code = c.country
JOIN spend s ON s.customer_id = c.id
ORDER BY co.continent, rank_in_continent, c.name
