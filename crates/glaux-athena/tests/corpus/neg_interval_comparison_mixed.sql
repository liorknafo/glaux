-- error: interval comparison
SELECT id FROM orders WHERE INTERVAL '1' MONTH < INTERVAL '30' DAY
