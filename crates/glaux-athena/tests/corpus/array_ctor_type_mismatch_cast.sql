-- error: Cannot find common type between bigint and varchar
-- The refusal is a planning-time TYPE_MISMATCH even when the varchar could
-- never be cast (glaux used to fail at run time with an Arrow cast error).
SELECT ARRAY[1, 'a']
