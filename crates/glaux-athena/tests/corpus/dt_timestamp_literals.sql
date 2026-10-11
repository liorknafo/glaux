-- Trino's zone-less timestamp literal forms, TIME text with milliseconds, and the clock functions' types.
SELECT TIMESTAMP '2024-01-05' AS date_only, TIMESTAMP '2024-01-05 10:00' AS no_seconds, TIMESTAMP '2024-01-05 10:00:00.5' AS one_fraction_digit,
       TIMESTAMP '2024-01-05 10:00:00.123' AS three_fraction_digits, TIMESTAMP '1600-01-01 00:00:00' AS before_nanosecond_window,
       TIMESTAMP '9999-12-31 23:59:59.999' AS far_future, TIME '10:00:00' AS time_text, TIME '10:00:00.5' AS time_fraction, CAST(TIME '10:00:00' AS VARCHAR) AS time_as_text,
       localtimestamp > TIMESTAMP '2024-01-01 00:00:00' AS local_is_after_2024,
       CAST(localtimestamp AS VARCHAR) LIKE '____-__-__ __:__:__.___' AS local_text_shape,
       now() >= current_timestamp AS clocks_agree
