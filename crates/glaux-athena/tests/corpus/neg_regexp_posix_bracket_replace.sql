-- error: INVALID_FUNCTION_ARGUMENT: regexp_replace
SELECT regexp_replace('ABC', '[[:alpha:]]', 'x')
