-- ARRAY literals, 1-based subscripts, and the array shims.
SELECT id,
       tags,
       cardinality(tags) AS n_tags,
       element_at(tags, 1) AS first_tag,
       element_at(tags, -1) AS last_tag,
       element_at(tags, 5) AS out_of_range,
       tags[1] AS subscript_first,
       contains(tags, 'vip') AS is_vip,
       array_join(tags, ',') AS joined,
       array_join(tags, ',', '?') AS joined_with_null,
       split('a,b,,c', ',') AS split_str,
       array_distinct(ARRAY[1, 2, 2, 3, 1]) AS distinct_ints,
       array_max(ARRAY[3, 9, 1]) AS mx, array_min(ARRAY[3, 9, 1]) AS mn,
       array_position(ARRAY['x', 'y', 'z'], 'y') AS pos_y,
       array_position(ARRAY['x', 'y', 'z'], 'q') AS pos_missing,
       array_remove(ARRAY[1, 2, 1, 3], 1) AS removed_all_ones,
       array_sort(ARRAY[3, 1, 2]) AS sorted,
       array_union(ARRAY[1, 2], ARRAY[2, 3]) AS unioned,
       arrays_overlap(tags, ARRAY['beta', 'gamma']) AS overlaps,
       flatten(ARRAY[ARRAY[1, 2], ARRAY[3]]) AS flat
FROM customers
ORDER BY id
