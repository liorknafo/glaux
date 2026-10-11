-- Trino declares from_unixtime(double) only; a varchar argument is a function-resolution error there, not an implicit parse.
-- error: Unexpected parameters (varchar) for function from_unixtime
SELECT from_unixtime('1700000000')
