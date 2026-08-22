-- error: Cannot add hour, minutes or seconds to a date
SELECT DATE '2024-01-05' + INTERVAL '1' HOUR
