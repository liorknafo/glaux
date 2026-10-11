-- error: quantified comparison (ALL)
-- sqlparser cannot even parse this operand; the refusal has to happen on
-- the token stream so the message names the predicate and not a raw
-- `Expected: ), found: ,`.
SELECT id FROM orders WHERE amount > ALL (VALUES 1, 2)
