-- error: NUMERIC_VALUE_OUT_OF_RANGE: bigint overflow on abs
-- Arrow reports an integer kernel's overflow as a compute error, which used
-- to reach the client as GENERIC_USER_ERROR with Arrow's array-type name.
SELECT abs(-9223372036854775808)
