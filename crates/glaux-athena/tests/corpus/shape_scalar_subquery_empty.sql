-- A scalar subquery that returns no rows is NULL, also over VALUES / literal sources whose columns are non-nullable.
SELECT (SELECT x FROM (VALUES (1)) t(x) WHERE x = 2) AS empty_values,
       (SELECT 1 WHERE false) AS empty_literal,
       (SELECT x FROM (VALUES (1)) t(x) WHERE x = 1) AS present,
       (SELECT max(id) FROM orders WHERE id < 0) AS empty_agg,
       coalesce((SELECT x FROM (VALUES (7)) t(x) WHERE x = 2), 0) AS coalesced
