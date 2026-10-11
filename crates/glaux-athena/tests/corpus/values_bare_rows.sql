-- Trino allows bare (unparenthesised) row expressions in VALUES.
SELECT x FROM (VALUES 1, 2, (3)) t(x) ORDER BY x
