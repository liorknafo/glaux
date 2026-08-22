-- A bare VALUES table reports _col0, _col1, ... like Athena.
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) ORDER BY 1 DESC
