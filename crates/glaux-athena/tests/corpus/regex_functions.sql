-- regexp_like, regexp_extract (group 0 and explicit groups), regexp_replace (global, 2- and 3-argument forms).
SELECT id,
       regexp_like(note, '\d{3}') AS has_3_digits,
       regexp_extract(note, '\d+') AS first_number,
       regexp_extract(note, '([a-z])=(\d+)', 2) AS first_value_after_eq,
       regexp_extract(note, 'ref ([A-Z]+)-(\d+)', 1) AS ref_prefix,
       regexp_replace(note, '\d') AS digits_removed,
       regexp_replace(note, '[aeiou]', '*') AS vowels_masked,
       regexp_replace(note, '(\w+)=(\d+)', '$2=$1') AS swapped
FROM orders
ORDER BY id
