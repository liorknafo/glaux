-- error: invalid regular expression
SELECT regexp_replace('abc', '(?=b)', 'x')
