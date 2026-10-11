-- Ranking arrays with no NULL elements still works, and every ranking path
-- agrees with the ordering operator: greatest/least, array_max/array_min,
-- max/min, array_sort, an aggregate ORDER BY, and BETWEEN.
SELECT greatest(ARRAY['a', 'b'], ARRAY['a', 'c']) AS greatest_arr,
       least(ARRAY['a', 'b'], ARRAY['a', 'c']) AS least_arr,
       array_max(ARRAY[ARRAY['a', 'b'], ARRAY['a', 'c']]) AS array_max_arr,
       array_min(ARRAY[ARRAY['a', 'b'], ARRAY['a', 'c']]) AS array_min_arr,
       array_sort(ARRAY[ARRAY['b'], ARRAY['a']]) AS sorted,
       array_sort(ARRAY['b', 'a', NULL]) AS sorted_with_null,
       ARRAY['a', 'b'] BETWEEN ARRAY['a'] AND ARRAY['z'] AS between_arr,
       ARRAY['a', 'b'] NOT BETWEEN ARRAY['a'] AND ARRAY['z'] AS not_between_arr
