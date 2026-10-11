-- error: NUMERIC_VALUE_OUT_OF_RANGE: Value -9223372036854775808 is out of range for abs(bigint)
-- Trino 411's MathFunctions.abs refuses the bigint minimum with the value
-- and the SQL type; DataFusion's checked kernel named its Arrow array type
-- instead ("Int64Array overflow on abs(-9223372036854775808)").
SELECT abs(CAST(-9223372036854775808 AS BIGINT))
