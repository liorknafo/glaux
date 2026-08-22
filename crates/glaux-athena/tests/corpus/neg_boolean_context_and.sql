-- error: Logical expression term must evaluate to a boolean (actual: bigint)
-- Trino types the operands of AND / OR as boolean.
SELECT true AND 1
