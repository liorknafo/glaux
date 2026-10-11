-- error: Padding string must not be empty
SELECT lpad('abc', 5, '')
