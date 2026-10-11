-- error: No group with name {y}
SELECT regexp_replace('abc', '(?<x>b)', '${y}')
