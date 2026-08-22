-- error: INVALID_FUNCTION_ARGUMENT: chr: Not a valid Unicode code point
-- DataFusion's text is "Execution error: invalid Unicode scalar value".
SELECT chr(1114112)
