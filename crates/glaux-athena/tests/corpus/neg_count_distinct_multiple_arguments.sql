-- error: for function count
-- Trino has count() and count(x) only, so this is an unknown overload;
-- DataFusion called it "This feature is not implemented", which reads as a
-- glaux TODO rather than a refusal Athena makes too.
SELECT count(DISTINCT status, customer_id) FROM orders
