-- A VALUES column mixing a double with an exact number is a double on Trino
-- (DataFusion alone reports decimal(30,15) and prints `1.000000000000000`);
-- integers stay integer and integer + decimal is decimal(11,1).
SELECT CAST(d AS VARCHAR) AS double_text, i, e
FROM (VALUES (CAST(1 AS DOUBLE), 1, 1),
             (CAST(1 AS DECIMAL(2,1)), 2, 1.5)) AS t(d, i, e)
