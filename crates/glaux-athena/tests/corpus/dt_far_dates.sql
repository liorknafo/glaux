-- Calendar arithmetic on dates outside Arrow's nanosecond window (1677-2262) returns values, never NULL.
SELECT date_add('day', 1, DATE '9999-12-31') AS day_after_9999,
       date_add('day', 1, DATE '1583-01-01') AS day_after_1583,
       date_add('day', 1, DATE '2262-04-12') AS day_after_2262,
       date_add('year', 1, DATE '9998-12-31') AS year_after,
       date_diff('day', DATE '2000-01-01', DATE '9999-12-31') AS days_to_9999,
       date_diff('year', DATE '1583-01-01', DATE '9999-12-31') AS years_span,
       date_trunc('month', DATE '9999-12-31') AS trunc_month_9999,
       date_trunc('year', DATE '1583-06-15') AS trunc_year_1583,
       CAST(DATE '9999-12-31' AS VARCHAR) AS far_text,
       date_add('day', 1, DATE '9999-12-31') > DATE '9999-12-31' AS ordered
