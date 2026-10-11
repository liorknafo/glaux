-- error: Cannot apply operator: bigint = varchar
SELECT * FROM customers JOIN (VALUES ('1')) AS v (id) USING (id)
