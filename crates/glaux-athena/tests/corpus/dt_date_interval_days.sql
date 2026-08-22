-- date ± interval works for whole days and year-month intervals; sub-day intervals are refused (neg_date_plus_hour).
SELECT DATE '2024-01-05' + INTERVAL '1' DAY AS plus_day,
       DATE '2024-01-05' - INTERVAL '2' MONTH AS minus_months,
       INTERVAL '1' DAY + DATE '2024-01-05' AS interval_first,
       DATE '2024-01-31' + INTERVAL '1' MONTH AS clamped_month,
       signup_date + INTERVAL '7' DAY AS column_plus_week,
       created_at + INTERVAL '1' HOUR AS timestamp_plus_hour
FROM customers JOIN orders ON orders.customer_id = customers.id
WHERE orders.id = 101
