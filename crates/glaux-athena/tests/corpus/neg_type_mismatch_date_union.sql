-- error: column 1 in UNION query has incompatible types: date, varchar
SELECT signup_date FROM customers UNION ALL SELECT '2024-01-01'
