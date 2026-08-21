-- Trino's `replace` has a separate branch for an empty `search`: it inserts
-- the replacement in front of every code point and at the end
-- (StringFunctions.replace, "With empty `search` we insert `replace` in
-- front of every character and [at] the end"), which its result-type
-- constraint `min(2147483647, x + z * (x + 1))` confirms. DataFusion's
-- `replace` returned the input unchanged.
SELECT replace('abc', '', 'X') AS empty_search,
       replace('', '', 'X') AS empty_both,
       replace('a👍', '', '-') AS code_points,
       length(replace('abc', '', 'XY')) AS widened_length,
       replace(note, '', '|') AS from_column,
       replace('a,b,,c', ',', ';') AS ordinary,
       replace('aaa', 'aa', 'b') AS non_overlapping,
       replace('abc', 'b') AS delete_form,
       replace('abc', '', NULL) AS null_replacement,
       replace(CAST(NULL AS VARCHAR), '', 'X') AS null_input
FROM orders
WHERE id = 104
