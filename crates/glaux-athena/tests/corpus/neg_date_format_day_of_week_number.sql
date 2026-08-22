-- error: INVALID_FUNCTION_ARGUMENT: date_format: %w not supported in date format string
-- Trino's `createDateTimeFormatter` maps the specifiers it supports onto
-- Joda fields and throws for `%D %U %u %V %w %X`; `%w` (numeric day of
-- week) is one chrono *does* have, so translating it would answer a query
-- Athena refuses.
SELECT date_format(TIMESTAMP '2024-01-05 22:30:45.123', '%w')
