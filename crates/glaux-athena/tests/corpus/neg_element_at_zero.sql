-- error: SQL array indices start at 1
SELECT element_at(ARRAY[1, 2], 0)
