-- error: INVALID_FUNCTION_ARGUMENT: date_parse
-- Joda resolves a bare weekday against its epoch base instant (Friday is
-- 1970-01-02); glaux cannot reproduce that, so it refuses instead of
-- guessing a date.
SELECT date_parse('Friday', '%W')
