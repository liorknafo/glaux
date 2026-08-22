-- A `VALUES` column mixing a typed NULL of a narrow integer type with bare
-- integer literals is `integer` on Trino. DataFusion plans the literals as
-- bigint first and widens the user's `CAST(... AS INTEGER)` to match
-- (`CAST(CAST(NULL AS Int32) AS Int64)`); glaux drops that widening cast so
-- the row types unify again, here and through UNION ALL.
SELECT x, y, z
FROM (VALUES (CAST(NULL AS INTEGER), CAST(NULL AS SMALLINT), CAST(1 AS BIGINT)),
             (5, 3, 4)) t(x, y, z)
