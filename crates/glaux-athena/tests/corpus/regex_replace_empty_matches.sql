-- Trino's JoniRegexpFunctions advances with getNextStart, which only skips
-- forward when the match itself was zero-width, so an empty match right
-- after a non-empty one is still replaced (Java's Matcher.replaceAll does
-- the same: "abc".replaceAll("b*", "X") is "XaXXcX"). Rust's
-- Regex::replace_all skips it, which cost one replacement per boundary.
SELECT regexp_replace('aaa', 'a*', 'X') AS all_consumed,
       regexp_replace('abc', 'b*', 'X') AS middle,
       regexp_replace('abc', 'c*', 'X') AS at_the_end,
       regexp_replace('ab12cd', '[0-9]*', '#') AS digits,
       regexp_replace('a b', '\s*', '-') AS spaces,
       length(regexp_replace('aaa', 'a*', 'X')) AS replacement_count,
       regexp_replace('abc', '', 'X') AS empty_pattern,
       regexp_replace('abc', 'x*', 'X') AS never_matches,
       regexp_replace('abc', 'b*', '') AS deleting,
       regexp_replace('', 'a*', 'X') AS empty_input,
       regexp_replace('a👍b', '👍*', '-') AS code_points,
       regexp_replace(note, ' *', '|') AS padded_column
FROM orders
WHERE id = 102
