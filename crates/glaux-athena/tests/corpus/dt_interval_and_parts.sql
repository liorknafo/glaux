-- INTERVAL arithmetic, date_trunc, date_add/date_diff (Trino argument order), EXTRACT, and the date-part functions.
SELECT id,
       created_at + INTERVAL '1' DAY AS next_day,
       created_at - INTERVAL '2' HOUR AS two_hours_earlier,
       date_trunc('month', created_at) AS month_start,
       date_trunc('week', created_at) AS week_start,
       date_add('day', 10, created_at) AS plus_10d,
       date_add('month', 1, created_at) AS plus_1m,
       date_add('year', -1, CAST(created_at AS DATE)) AS minus_1y_date,
       date_diff('day', TIMESTAMP '2024-01-01 00:00:00', created_at) AS days_since_ny,
       date_diff('hour', created_at, TIMESTAMP '2024-06-01 00:00:00') AS hours_to_june,
       date_diff('month', DATE '2024-01-31', CAST(created_at AS DATE)) AS months_since_jan31,
       EXTRACT(YEAR FROM created_at) AS ex_year,
       year(created_at) AS y, month(created_at) AS m, day(created_at) AS d,
       day_of_month(created_at) AS dom, hour(created_at) AS h, minute(created_at) AS mi,
       second(created_at) AS s, quarter(created_at) AS q, week(created_at) AS w,
       week_of_year(created_at) AS woy, day_of_week(created_at) AS dow,
       dow(created_at) AS dow2, day_of_year(created_at) AS doy, doy(created_at) AS doy2,
       date(created_at) AS just_date
FROM orders
WHERE created_at IS NOT NULL
ORDER BY id
