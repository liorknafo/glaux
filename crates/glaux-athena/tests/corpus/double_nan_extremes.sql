-- Trino's max side (greatest / max / array_max) ranks NaN smallest
-- (COMPARISON_UNORDERED_FIRST); the min side ranks it largest, so neither
-- ever prefers NaN over a real number.
SELECT greatest(1e0, CAST('NaN' AS DOUBLE)) AS g,
       least(1e0, CAST('NaN' AS DOUBLE)) AS l,
       max(f) AS mx,
       min(f) AS mn,
       array_max(ARRAY[1e0, CAST('NaN' AS DOUBLE)]) AS amx,
       array_min(ARRAY[1e0, CAST('NaN' AS DOUBLE)]) AS amn
FROM (VALUES (1e0), (CAST('NaN' AS DOUBLE)), (0.5e0)) t(f)
