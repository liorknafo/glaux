-- `$` is an assertion glaux can only emulate at the end of a match; a pattern that continues after it is refused by name.
-- error: is followed by a part of the pattern that can match text
SELECT regexp_replace('ab' || chr(10), '(b$)x?', 'y')
