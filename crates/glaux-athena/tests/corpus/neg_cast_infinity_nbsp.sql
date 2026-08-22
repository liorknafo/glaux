-- error: Cannot cast 'Infinity
SELECT CAST('Infinity ' || chr(160) AS REAL)
