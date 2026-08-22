-- error: column 1 in UNION query has incompatible types: bigint, varchar
SELECT id FROM customers UNION SELECT name FROM customers
