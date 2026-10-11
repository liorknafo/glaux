-- error: ARRAY comparison not supported for arrays with null elements
SELECT ARRAY[1, NULL] < ARRAY[1, 2]
