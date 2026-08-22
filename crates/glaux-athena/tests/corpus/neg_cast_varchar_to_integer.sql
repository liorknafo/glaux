-- error: Cannot cast '1.5' to integer
-- Arrow said "Cannot cast string '1.5' to value of Int32 type".
SELECT CAST('1.5' AS INTEGER)
