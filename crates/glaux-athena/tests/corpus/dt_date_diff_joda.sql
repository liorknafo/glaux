-- date_diff for month/quarter/year follows Joda-Time's getDifference, as Trino does: a minuend on the last day of its month counts a later subtrahend day as a full month.
SELECT date_diff('month', DATE '2024-01-31', DATE '2024-02-29') AS jan31_feb29,
       date_diff('month', DATE '2024-01-30', DATE '2024-02-29') AS jan30_feb29,
       date_diff('month', DATE '2024-03-31', DATE '2024-04-30') AS mar31_apr30,
       date_diff('month', DATE '2024-02-29', DATE '2024-01-31') AS feb29_jan31,
       date_diff('month', DATE '2024-01-31', DATE '2024-02-28') AS jan31_feb28,
       date_diff('month', DATE '2024-01-15', DATE '2024-03-14') AS partial_month,
       date_diff('quarter', DATE '2024-01-31', DATE '2024-04-30') AS quarter_end_to_end,
       date_diff('year', DATE '2023-03-01', DATE '2024-02-29') AS year_short_by_a_day,
       date_diff('year', DATE '2024-02-29', DATE '2025-02-28') AS leap_to_common,
       date_diff('year', DATE '2023-01-01', DATE '2024-01-01') AS full_year,
       date_diff('month', TIMESTAMP '2024-01-31 12:00:00', TIMESTAMP '2024-02-29 11:59:59') AS time_of_day_counts,
       date_diff('week', DATE '2024-01-01', DATE '2024-01-15') AS weeks,
       date_diff('hour', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 05:30:00') AS hours_truncated
