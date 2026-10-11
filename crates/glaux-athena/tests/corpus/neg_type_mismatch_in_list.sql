-- error: Cannot apply operator: bigint = varchar
SELECT id FROM orders WHERE id IN ('101', '102')
