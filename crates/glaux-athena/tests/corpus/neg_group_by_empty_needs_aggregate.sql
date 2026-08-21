-- error: must appear in the GROUP BY clause
-- `GROUP BY ()` is still a grouping clause: a bare column is not allowed.
SELECT status FROM orders GROUP BY ()
