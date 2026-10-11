-- error: NUMERIC_VALUE_OUT_OF_RANGE: Value -128 is out of range for abs(tinyint)
-- The column form: the offending value comes from the batch, not from a
-- folded constant.
SELECT abs(x) FROM (VALUES (CAST(-128 AS TINYINT)), (CAST(1 AS TINYINT))) t(x)
