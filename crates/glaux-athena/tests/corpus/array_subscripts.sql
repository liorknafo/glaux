-- Subscripts are 1-based and checked; element_at is lenient at the ends.
SELECT ARRAY[10, 20, 30][2] AS second, ARRAY[ARRAY[1, 2], ARRAY[3]][1][2] AS nested,
       element_at(ARRAY[10, 20, 30], -1) AS last, element_at(ARRAY[10, 20, 30], 4) AS beyond,
       element_at(ARRAY[10, 20, 30], -4) AS before, element_at(ARRAY['a', 'b'], 1) AS first_str
