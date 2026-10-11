-- `TRIM(LEADING FROM x)` (no trim characters) is valid Trino and trims
-- whitespace; sqlparser cannot parse the spelling, so the tokens become the
-- equivalent ltrim / rtrim / trim call before parsing. The forms that do
-- parse are unchanged.
SELECT '[' || trim(LEADING FROM '  a  ') || ']' AS bare_leading,
       '[' || trim(TRAILING FROM '  a  ') || ']' AS bare_trailing,
       '[' || trim(BOTH FROM '  a  ') || ']' AS bare_both,
       '[' || trim('  a  ') || ']' AS plain,
       '[' || trim(LEADING FROM CAST(note AS VARCHAR)) || ']' AS bare_leading_column,
       trim(BOTH 'x' FROM 'xxaxx') AS with_characters,
       trim(LEADING 'x' FROM 'xxaxx') AS leading_characters
FROM orders WHERE id = 102
