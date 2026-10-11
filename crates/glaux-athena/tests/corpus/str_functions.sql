-- String functions with Trino semantics (1-based positions, NULL-propagating concat).
SELECT id,
       substr(note, 1, 5) AS sub5,
       substring(note, 7) AS from7,
       substring(note FROM 2 FOR 3) AS sub_syntax,
       strpos(note, ':') AS colon_at,
       position('#' IN note) AS hash_at,
       split_part(note, ',', 2) AS second_part,
       length(note) AS len,
       lower(note) AS lo, upper(note) AS up,
       trim(note) AS trimmed, ltrim(note) AS ltrimmed, rtrim(note) AS rtrimmed,
       trim(BOTH 'x' FROM 'xxhixx') AS trim_both,
       replace(note, 'a', 'A') AS replaced, replace(note, ',') AS commas_removed,
       reverse(note) AS rev,
       lpad(CAST(id AS VARCHAR), 5, '0') AS padded, rpad('ab', 4, '-') AS rpadded,
       starts_with(note, 'Order') AS is_order,
       concat(note, '!') AS with_bang,
       concat('x', note, 'y') AS wrapped,
       'a' || NULL AS null_concat,
       chr(65) AS a_char, codepoint('A') AS a_code,
       levenshtein_distance('kitten', 'sitting') AS lev,
       translate('abc', 'ab', 'xy') AS translated
FROM orders
ORDER BY id
