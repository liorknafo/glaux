-- error: Pattern has 1 groups. Cannot access group 2
SELECT regexp_extract('abc123', '(\d+)', 2)
