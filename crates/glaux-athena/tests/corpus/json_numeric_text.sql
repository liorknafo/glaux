-- JSON numbers are returned as written by json_extract_scalar; json_parse produces Trino's canonical text (sorted keys, exact integers).
SELECT json_extract_scalar('{"a": 1.50}', '$.a') AS keeps_trailing_zero,
       json_extract_scalar('{"a": 1e2}', '$.a') AS keeps_exponent,
       json_extract_scalar('{"a": 1E+2}', '$.a') AS keeps_exponent_sign,
       json_extract_scalar('{"a": 1.0e-7}', '$.a') AS keeps_small_exponent,
       json_extract_scalar('{"a": 100000000000000000000000}', '$.a') AS keeps_big_integer,
       json_extract_scalar('{"a": 123456789.123456789}', '$.a') AS keeps_long_fraction,
       json_extract_scalar('{"a": 1, "a": 2}', '$.a') AS first_duplicate_key,
       json_parse('{"b": 1, "a": 2}') AS sorted_keys,
       json_parse('{"b": 123456789012345678901234567890}') AS exact_big_integer,
       json_parse('{"a": 1, "a": 2}') AS last_duplicate_wins,
       json_parse('{"z": [1.50, 1e2, true, null, "s\u00e9"], "a": {"y": 1, "x": -0}}') AS canonical_nested,
       json_extract('{"b": [1, 2.50], "a": {"x": null}}', '$.b') AS extracted_array,
       json_format(json_parse('{"z":1,"a":"q\"\n"}')) AS formatted,
       json_extract_scalar(profile, '$.age') AS age_text
FROM customers
WHERE id = 1
