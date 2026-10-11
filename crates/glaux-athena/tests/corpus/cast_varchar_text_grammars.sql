-- Which varchar casts trim, and which do not, follows the Java parser Trino calls: Double.parseDouble trims (DOUBLE / REAL), Long.parseLong does not (BIGINT / INTEGER / SMALLINT / TINYINT), and the DATE / TIMESTAMP casts trim because Trino trims the slice before matching its pattern.
SELECT CAST(' 1.5 ' AS DOUBLE) AS double_trims,
       CAST('  12  ' AS DOUBLE) AS double_trims_integer_text,
       TRY_CAST(' 12 ' AS BIGINT) AS bigint_does_not_trim,
       TRY_CAST('12' || chr(10) AS BIGINT) AS bigint_no_trailing_newline,
       TRY_CAST(' 12' AS INTEGER) AS integer_does_not_trim,
       TRY_CAST('12 ' AS SMALLINT) AS smallint_does_not_trim,
       TRY_CAST(' 12 ' AS TINYINT) AS tinyint_does_not_trim,
       CAST('12' AS BIGINT) AS bigint_plain_text,
       CAST('+12' AS INTEGER) AS integer_signed_text,
       CAST('-0012' AS SMALLINT) AS smallint_leading_zeros,
       CAST(' 2024-01-05 ' AS DATE) AS date_trims,
       CAST(' 2024-01-05 10:00:00 ' AS TIMESTAMP) AS timestamp_trims,
       TRY_CAST(' true ' AS BOOLEAN) AS boolean_does_not_trim
