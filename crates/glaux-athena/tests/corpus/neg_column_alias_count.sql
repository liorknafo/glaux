-- error: Column alias list has 1 entries but relation has 2 columns
-- Trino's wording; DataFusion said "Source table contains 2 columns but only
-- 1 names given as column alias".
SELECT x FROM (VALUES (1, 2)) AS t(x)
