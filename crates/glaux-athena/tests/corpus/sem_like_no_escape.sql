-- LIKE has no default escape character in Trino: a backslash is a literal unless ESCAPE names it.
SELECT 'a_c' LIKE 'a\_c' AS backslash_underscore_literal,
       'a\bc' LIKE 'a\_c' AS backslash_then_wildcard,
       'a%c' LIKE 'a\%c' AS backslash_percent_literal,
       'a\b' LIKE 'a\\b' AS two_backslashes_are_two,
       'a\\b' LIKE 'a\\b' AS two_backslashes_match,
       'a_c' LIKE 'a#_c' ESCAPE '#' AS escaped_underscore,
       'abc' LIKE 'a#_c' ESCAPE '#' AS escaped_underscore_no_match,
       'a#c' LIKE 'a##c' ESCAPE '#' AS escaped_escape,
       'a\c' LIKE 'a\\c' ESCAPE '\' AS backslash_as_escape,
       'abc' LIKE 'a%' AS plain_prefix,
       note LIKE '%#%%' ESCAPE '#' AS note_has_percent,
       note LIKE '%\%' AS note_ends_with_backslash
FROM orders
WHERE id = 101
