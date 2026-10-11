-- `%x` is the week-year that `%v`'s Monday-first week belongs to (Joda's
-- `xxxx`), a Trino specifier glaux used to refuse. 2023-01-01 is a Sunday,
-- so it falls in week 52 of 2022.
SELECT date_format(TIMESTAMP '2024-01-05 10:30:00', '%x-%v') AS mid_year,
       date_format(TIMESTAMP '2023-01-01 00:00:00', '%x-%v') AS previous_weekyear,
       date_format(TIMESTAMP '2024-12-30 00:00:00', '%x-%v') AS next_weekyear
