-- Array operators with Trino's NULL-element rules.
SELECT ARRAY[1, NULL] = ARRAY[1, NULL] AS eq_null_null,
       ARRAY[1, NULL] = ARRAY[1, 2] AS eq_null_value,
       ARRAY[1, NULL] <> ARRAY[1, NULL] AS neq_null_null,
       ARRAY[1, NULL] = ARRAY[2, NULL] AS eq_definite_mismatch,
       ARRAY[1, NULL] IS DISTINCT FROM ARRAY[1, NULL] AS distinct_null_null,
       ARRAY[1, 2] = ARRAY[1, 2] AS eq_equal,
       ARRAY[1, 2] = ARRAY[1] AS eq_length,
       ARRAY[1, 2] < ARRAY[1, 3] AS lt,
       ARRAY[1] < ARRAY[1, 0] AS prefix_lt,
       ARRAY[2] > ARRAY[1, 9] AS gt,
       ARRAY[1, 2] >= ARRAY[1, 2] AS gte,
       ARRAY[1] = ARRAY[1.0] AS eq_mixed_numeric,
       ARRAY['a', 'b'] = ARRAY['a', 'b'] AS eq_strings,
       ARRAY[ARRAY[1, NULL]] = ARRAY[ARRAY[1, NULL]] AS eq_nested_null,
       tags = ARRAY['vip', 'early'] AS eq_column,
       NULL = ARRAY[1] AS eq_null_array
FROM customers WHERE id = 1
