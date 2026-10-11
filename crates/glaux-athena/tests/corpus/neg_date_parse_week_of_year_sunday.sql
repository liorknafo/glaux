-- error: INVALID_FUNCTION_ARGUMENT: date_parse: %U not supported in date format string
-- The Sunday-first week fields (`%U`, `%V`, `%X`), `%u`, and `%D` throw in
-- Trino too, on the parse side as much as the format side.
SELECT date_parse('2024 01', '%Y %U')
