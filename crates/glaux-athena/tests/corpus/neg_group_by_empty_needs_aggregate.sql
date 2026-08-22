-- error: must be an aggregate expression or appear in GROUP BY clause
-- `GROUP BY ()` is still a grouping clause: a bare column is not allowed.
SELECT status FROM orders GROUP BY ()
