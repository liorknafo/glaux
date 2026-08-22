-- error: Cannot apply operator: bigint = varchar
SELECT CASE id WHEN '1' THEN 'one' END FROM customers
