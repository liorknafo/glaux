-- error: NUMERIC_VALUE_OUT_OF_RANGE: tinyint division overflow: -128 / -1
SELECT CAST(-128 AS TINYINT) / CAST(-1 AS TINYINT)
