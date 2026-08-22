-- Joda's parse bucket starts at 1970-01-01T00:00:00 and applies the parsed
-- fields on top, so every field the format does not name keeps its epoch
-- default. chrono needs hour + minute (and AM/PM for a 12-hour field) to
-- build a time at all, and DataFusion's to_timestamp quietly fell back to
-- midnight for the rest, dropping the whole time of day.
SELECT date_parse('2024-01-05 10', '%Y-%m-%d %H') AS hour_only,
       date_parse('2024-01-05 10:30', '%Y-%m-%d %h:%i') AS clock_hour_no_meridiem,
       date_parse('2024-01-05 10 PM', '%Y-%m-%d %h %p') AS clock_hour_pm,
       date_parse('12:30 AM', '%h:%i %p') AS time_only_is_the_epoch_day,
       date_parse('2024-01', '%Y-%m') AS year_month,
       date_parse('2024', '%Y') AS year_only,
       date_parse('03-15', '%m-%d') AS month_day,
       date_parse('2024-01-05 10:30', '%Y-%m-%d %H:%i') AS complete_is_unchanged,
       parse_datetime('2024-01-05 10', 'yyyy-MM-dd HH') AS joda_hour_only,
       parse_datetime('2024-01-05 10:30', 'yyyy-MM-dd hh:mm') AS joda_clock_hour,
       parse_datetime('2024-01-05 10 PM', 'yyyy-MM-dd hh a') AS joda_clock_hour_pm,
       hour(date_parse(substr(note, 1, 13), '%Y-%m-%d %H')) AS from_column
FROM orders
WHERE id = 105
