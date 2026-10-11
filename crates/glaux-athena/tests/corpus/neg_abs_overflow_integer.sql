-- error: NUMERIC_VALUE_OUT_OF_RANGE: Value -2147483648 is out of range for abs(integer)
-- Trino has one abs per integer type, each naming its own type.
SELECT abs(CAST(-2147483648 AS INTEGER))
