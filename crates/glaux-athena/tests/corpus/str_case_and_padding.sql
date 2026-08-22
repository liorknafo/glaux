-- upper / lower map per code point (Java's Character.toUpperCase); lpad / rpad count code points and truncate; varchar(n) truncates only varchar sources.
SELECT upper('straße') AS sharp_s_kept, lower('İstanbul') AS dotted_i, upper('ﬁ') AS ligature_kept, lower('ΟΔΥΣΣΕΥΣ') AS no_final_sigma,
       upper(name) AS upper_name, lower('ÀÉÎ') AS lower_accents,
       lpad('abc', 5, 'xy') AS lpad_cycle, rpad('abc', 6, 'xy') AS rpad_cycle, lpad('abcdef', 3, 'x') AS lpad_truncates, lpad('abc', 3, '') AS lpad_no_padding_needed,
       codepoint('a') AS codepoint_a, codepoint('é') AS codepoint_accent,
       CAST('abcdef' AS VARCHAR(2)) AS varchar_truncates, CAST(12 AS VARCHAR(2)) AS int_fits, TRY_CAST(12345 AS VARCHAR(2)) AS try_int_too_long,
       CAST(1.5 AS VARCHAR(3)) AS dec_fits, CAST(DATE '2024-01-05' AS VARCHAR(10)) AS date_fits
FROM customers
WHERE id = 1
