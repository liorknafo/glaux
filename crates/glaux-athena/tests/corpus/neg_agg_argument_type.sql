-- error: Unexpected parameters (varchar) for function sum
-- Trino has no varchar overload of sum; DataFusion's signature matcher used
-- to leak an `Internal error: Function 'sum' failed to match any signature`
-- ending in an invitation to file a DataFusion bug report.
SELECT sum(status) FROM orders
