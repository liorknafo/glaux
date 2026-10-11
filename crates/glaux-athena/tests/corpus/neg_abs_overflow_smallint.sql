-- error: NUMERIC_VALUE_OUT_OF_RANGE: Value -32768 is out of range for abs(smallint)
SELECT abs(CAST(-32768 AS SMALLINT))
