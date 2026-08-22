-- The two format languages pivot a two-digit year differently, and only
-- one of them moves. Joda's DateTimeFormat builds a two-character `y`/`Y`
-- token as `appendTwoDigitYear(new DateTime().getYear() - 30)`, so
-- `parse_datetime` reads `69` as 1969 (window [now-80, now+19]); Trino's
-- MySQL-style formatter uses `appendTwoDigitYear(PIVOT_YEAR)` with a fixed
-- PIVOT_YEAR of 2020, so `date_parse` reads `69` as 2069. glaux mapped
-- Joda's `yy` onto chrono's `%y` (the fixed window) and answered 2069 for
-- both. `xx` pivots the same way off the current week-year.
--
-- The values here are the ones both windows agree on until 2049; the
-- moving edge is checked in the unit test, which computes it from the
-- current year.
SELECT parse_datetime('69-01-05', 'yy-MM-dd') AS joda_69,
       parse_datetime('70-01-05', 'yy-MM-dd') AS joda_70,
       parse_datetime('99-01-05', 'yy-MM-dd') AS joda_99,
       parse_datetime('2046-01-05', 'yyyy-MM-dd') AS joda_four_digit,
       parse_datetime('69 02 1', 'xx ww e') AS joda_weekyear_69,
       date_parse('69-01-05', '%y-%m-%d') AS mysql_69,
       date_parse('70-01-05', '%y-%m-%d') AS mysql_70
