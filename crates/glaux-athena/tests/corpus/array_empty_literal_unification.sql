-- An empty `ARRAY[]` is `array(unknown)` in Trino, which unifies with the
-- sibling operand's element type. `contains` / `arrays_overlap` used to cast
-- the *value* to the list's element type instead, and Arrow casts nothing to
-- `Null`, so `contains(ARRAY[], 1)` died with `Casting from Int32 to Null
-- not supported` — argument-order dependent, since the mirrored
-- `arrays_overlap(ARRAY[1], ARRAY[])` already worked.
SELECT contains(ARRAY[], 1) AS contains_empty_int,
       contains(ARRAY[], 'a') AS contains_empty_varchar,
       contains(ARRAY[NULL], 1) AS contains_all_null,
       contains(ARRAY[], NULL) AS contains_null_value,
       arrays_overlap(ARRAY[], ARRAY[1]) AS overlap_empty_left,
       arrays_overlap(ARRAY[1], ARRAY[]) AS overlap_empty_right,
       arrays_overlap(ARRAY[], ARRAY[]) AS overlap_both_empty,
       arrays_overlap(ARRAY[NULL], ARRAY[1]) AS overlap_all_null,
       contains(ARRAY[], tags) AS contains_empty_array_of_array,
       cardinality(ARRAY[]) AS empty_cardinality,
       array_position(ARRAY[], 1) AS empty_position,
       ARRAY[] || ARRAY[1] AS concatenated
FROM customers
WHERE id = 3
