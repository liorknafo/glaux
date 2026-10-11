-- A `[` inside a character class opens a nested class in Java (and in
-- glaux's engine), so the unions and the colons that are *not* a POSIX
-- bracket expression keep working after that construct was refused.
SELECT regexp_like('b', '[a[bc]]') AS nested_union,
       regexp_replace('a:b', '[a:b]', 'x') AS colon_in_class,
       regexp_extract('12:30', '[0-9]+[:][0-9]+') AS colon_class,
       regexp_like('h', '[:alpha:]') AS top_level_looks_posix,
       regexp_like('z', '[:alpha:]') AS top_level_not_a_letter
