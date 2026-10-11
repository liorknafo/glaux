-- error: LIMIT takes a non-negative row count
-- Trino's grammar has no sign in LIMIT, so this is a parse error there;
-- DataFusion accepted the literal and failed inside `eliminate_limit`,
-- leaking the optimizer rule's name.
SELECT id FROM orders LIMIT -1
