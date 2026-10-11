-- CAST semantics: HALF_UP rounding into integer types, Trino text forms for VARCHAR, VARCHAR(n) truncation, TRY_CAST NULL on overflow.
SELECT CAST(2.5 AS BIGINT) AS half_up, CAST(-2.5 AS BIGINT) AS half_up_neg,
       CAST(CAST(120.5 AS DOUBLE) AS INTEGER) AS dbl_half_up, CAST(CAST(2.4 AS DOUBLE) AS BIGINT) AS dbl_down,
       CAST(CAST(-0.5 AS DOUBLE) AS SMALLINT) AS small_neg, CAST('42' AS BIGINT) AS from_str,
       TRY_CAST(CAST(1e30 AS DOUBLE) AS BIGINT) AS try_overflow, TRY_CAST('x' AS INTEGER) AS try_bad,
       CAST(TIMESTAMP '2024-01-05 10:30:00' AS VARCHAR) AS ts_text, CAST(DATE '2024-01-05' AS VARCHAR) AS date_text,
       CAST('abc' AS VARCHAR(2)) AS truncated, CAST('abc' AS VARCHAR(10)) AS not_truncated,
       CAST(CAST(1e20 AS DOUBLE) AS VARCHAR) AS dbl_text, CAST(CAST(2 AS DOUBLE) AS VARCHAR) AS dbl_whole,
       CAST(1.50 AS VARCHAR) AS dec_text, CAST(true AS VARCHAR) AS bool_text, CAST(12 AS VARCHAR) AS int_text,
       CAST(CAST(0.1 AS DOUBLE) + CAST(0.2 AS DOUBLE) AS VARCHAR) AS float_sum_text,
       CAST(NULL AS VARCHAR) AS null_text
