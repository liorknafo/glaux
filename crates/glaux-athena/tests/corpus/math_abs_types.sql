-- abs over every numeric type Trino accepts: the result keeps the argument's
-- type (a decimal keeps its precision and scale), and only the minimum of an
-- integer type overflows (see neg_abs_overflow*).
SELECT abs(CAST(-128 AS TINYINT)+CAST(1 AS TINYINT)) AS tiny,
       abs(CAST(-32767 AS SMALLINT)) AS small,
       abs(CAST(-2147483647 AS INTEGER)) AS int_,
       abs(CAST(-9223372036854775807 AS BIGINT)) AS big,
       abs(CAST(-1.5 AS DOUBLE)) AS dbl,
       abs(CAST(-2.5 AS REAL)) AS rea,
       abs(CAST(-2.50 AS DECIMAL(5,2))) AS dec_,
       abs(CAST(NULL AS BIGINT)) AS nul
