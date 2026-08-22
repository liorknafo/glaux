-- A `$` whose match could stop either side of the final newline: Joni picks one by backtracking order, so glaux refuses instead of guessing.
-- error: $` against text ending in a newline
SELECT regexp_extract('ab' || chr(10), '[a-z' || chr(10) || ']*$')
