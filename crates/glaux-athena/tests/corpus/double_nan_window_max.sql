SELECT f, max(f) OVER () AS wmx
FROM (VALUES (1e0), (CAST('NaN' AS DOUBLE))) t(f)
ORDER BY f
