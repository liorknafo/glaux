-- Array functions with NULL elements: Trino's NULL rules and Trino's element text forms.
SELECT array_max(ARRAY[1, NULL, 3]) AS max_with_null, array_min(ARRAY[1, NULL, 3]) AS min_with_null,
       array_max(ARRAY['a', NULL]) AS max_str_null, array_min(ARRAY[NULL, 2.5]) AS min_dec_null,
       array_max(ARRAY[3, 1, 2]) AS max_ints, array_min(ARRAY['b', 'a']) AS min_strs, array_max(tags) AS max_tags,
       array_remove(ARRAY[1, NULL], 1) AS remove_keeps_null, cardinality(array_remove(ARRAY[1, NULL], 1)) AS remove_keeps_null_n, element_at(array_remove(ARRAY[1, NULL], 1), 1) IS NULL AS kept_element_is_null, array_remove(ARRAY[1, NULL, 1], 1) AS remove_all_keeps_null,
       array_remove(ARRAY[1, 2, 1], 1) AS remove_all, array_remove(tags, 'vip') AS remove_from_column, array_remove(ARRAY[1, 2], NULL) AS remove_null_is_null,
       array_position(ARRAY[1, NULL], NULL) AS position_of_null, array_position(ARRAY[1, NULL], 2) AS position_missing, array_position(ARRAY[1, NULL], 1) AS position_found,
       arrays_overlap(ARRAY[NULL], ARRAY[NULL]) AS overlap_nulls, contains(ARRAY[NULL], NULL) AS contains_null_null,
       array_join(ARRAY[1e0, 2.5e0], ',') AS join_doubles, array_join(ARRAY[TIMESTAMP '2024-01-05 10:00:00'], ',') AS join_timestamp,
       array_join(ARRAY[1, NULL, 3], ',', 'x') AS join_with_replacement, array_join(ARRAY[1, NULL, 3], ',') AS join_skips_null,
       array_join(ARRAY[1.5, 2.25], ';') AS join_decimals, array_join(ARRAY[DATE '2024-01-05', NULL], '|', '-') AS join_dates, array_join(tags, '+') AS join_column
FROM customers
WHERE id = 4
