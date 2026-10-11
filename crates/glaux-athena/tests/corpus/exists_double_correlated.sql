-- Correlated EXISTS whose predicate is a float equality: the analyzer
-- routes it through the IEEE comparison UDF before DataFusion
-- decorrelates, so the NaN row matches nothing (a hash semi-join on the
-- raw column would match NaN with NaN).
SELECT a.i
FROM (VALUES (1, 1e0), (2, CAST('NaN' AS DOUBLE))) a(i, f)
WHERE EXISTS (
  SELECT 1 FROM (VALUES (1e0), (CAST('NaN' AS DOUBLE))) b(g)
  WHERE b.g = a.f
)
ORDER BY a.i
