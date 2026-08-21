-- Trino's `GROUP BY ()` is the empty grouping set: one global group.
-- sqlparser parses it as an empty tuple, which DataFusion refused with
-- `This feature is not implemented: Empty tuple not supported yet`.
SELECT count(*) AS n, sum(amount) AS total FROM orders GROUP BY ()
