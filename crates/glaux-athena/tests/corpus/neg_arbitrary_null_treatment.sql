-- error: INVALID_FUNCTION_ARGUMENT: arbitrary
-- Trino's grammar has no null-treatment clause on `arbitrary`, and glaux
-- must not accept `RESPECT NULLS` silently: the rewrite always asks for
-- IGNORE NULLS, so honouring the clause is impossible and ignoring it
-- would answer the opposite of what it asks for.
SELECT arbitrary(amount) RESPECT NULLS FROM orders
