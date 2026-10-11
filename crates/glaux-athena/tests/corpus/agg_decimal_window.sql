-- Windowed decimal avg / sum keep Trino's types too.
SELECT id, CAST(amount AS DECIMAL(10,2)) AS amount_dec,
       avg(CAST(amount AS DECIMAL(10,2))) OVER () AS avg_all,
       sum(CAST(amount AS DECIMAL(10,2))) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS running_sum,
       avg(CAST(amount AS DECIMAL(10,2))) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS running_avg,
       avg(x) AS avg_of_literals
FROM orders, (VALUES (1.5), (2.5)) t(x)
GROUP BY id, amount
ORDER BY id
