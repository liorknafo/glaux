-- Trino's || propagates a NULL *array* operand to NULL, while a NULL
-- element operand is appended as a NULL element.
SELECT ARRAY[1, 2] || CAST(NULL AS ARRAY(INTEGER)) AS a,
       CAST(NULL AS ARRAY(INTEGER)) || ARRAY[1] AS b,
       ARRAY[1] || CAST(NULL AS INTEGER) AS c,
       CAST(NULL AS INTEGER) || ARRAY[1] AS d,
       ARRAY[1] || ARRAY[2, 3] AS e
