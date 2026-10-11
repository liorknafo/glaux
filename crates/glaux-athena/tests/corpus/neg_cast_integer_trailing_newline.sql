-- A trailing newline is whitespace Java's Long.parseLong refuses too (Arrow's string parser would have trimmed it and returned 12).
-- error: to integer
SELECT CAST('12' || chr(10) AS INTEGER)
