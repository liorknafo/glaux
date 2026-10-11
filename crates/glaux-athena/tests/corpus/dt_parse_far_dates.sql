-- `date_parse` / `parse_datetime` build the millisecond value from Joda's
-- own field defaults instead of going through DataFusion's nanosecond
-- `to_timestamp`, so they carry no Arrow nanosecond window (1677-2262):
-- dates outside it parse into ordinary values, as they do on Trino, whose
-- ISO chronology is proleptic Gregorian like chrono's.
SELECT date_parse('1000-01-01', '%Y-%m-%d') AS far_past,
       date_parse('9999-12-31 23:59:59', '%Y-%m-%d %H:%i:%s') AS far_future,
       parse_datetime('1000-01-01', 'yyyy-MM-dd') AS far_past_zoned,
       year(date_parse('1000-01-01', '%Y-%m-%d')) AS far_past_year,
       date_diff('day', date_parse('1000-01-01', '%Y-%m-%d'),
                        date_parse('1000-01-31', '%Y-%m-%d')) AS far_past_span
