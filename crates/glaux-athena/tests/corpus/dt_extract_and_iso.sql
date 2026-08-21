-- EXTRACT uses Trino field numbering; from_iso8601_* are strict; %f parses 1-9 digits; Joda Z prints the UTC offset.
SELECT EXTRACT(DOW FROM DATE '2024-01-07') AS sunday_is_7, EXTRACT(DAY_OF_WEEK FROM DATE '2024-01-08') AS monday_is_1,
       EXTRACT(DOY FROM DATE '2024-02-01') AS doy, EXTRACT(WEEK FROM DATE '2024-01-07') AS iso_week,
       EXTRACT(HOUR FROM TIMESTAMP '2024-01-07 13:45:10') AS h, EXTRACT(DAY_OF_MONTH FROM DATE '2024-01-07') AS dom,
       day_of_week(DATE '2024-01-07') AS dow_fn,
       from_iso8601_timestamp('2024-01-01T10:00:00.5+02:00') AS iso_offset, from_iso8601_timestamp('2024-03-01') AS iso_date_only,
       from_iso8601_timestamp('2024-03-01T08:30Z') AS iso_no_seconds, from_iso8601_date('2024-02-29') AS iso_date,
       date_parse('2024-03-15 12:00:00.123', '%Y-%m-%d %H:%i:%s.%f') AS frac3,
       date_parse('2024-03-15 12:00:00.123456', '%Y-%m-%d %H:%i:%s.%f') AS frac6,
       date_format(TIMESTAMP '2024-03-15 12:00:00.123', '%H:%i:%s.%f') AS fmt_frac,
       format_datetime(TIMESTAMP '2024-03-15 12:00:00', 'yyyy-MM-dd HH:mm Z') AS joda_zone,
       format_datetime(TIMESTAMP '2024-03-15 12:00:00', 'HH:mm ZZ') AS joda_zone_colon
