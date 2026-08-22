-- A bare NULL literal is Trino's `unknown` type, and the Athena result
-- metadata reports that name; a CAST gives the column a real type.
SELECT NULL AS bare_null, CAST(NULL AS BIGINT) AS typed_null, NULL IS NULL AS is_null
