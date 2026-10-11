-- error: POSIX bracket expression `[:digit:]`
-- The same refusal in regexp_extract (glaux answered '123' where Trino
-- answers NULL) — and in regexp_replace, which answered 'xxx' for
-- regexp_replace('ABC', '[[:alpha:]]', 'x') where Trino answers 'ABC'.
SELECT regexp_extract('xx123xx', '[[:digit:]]+')
