-- error: NUMERIC_VALUE_OUT_OF_RANGE: tinyint multiplication overflow: 127 * 2
SELECT CAST(127 AS TINYINT) * CAST(2 AS TINYINT)
