-- Typed reads from the NDJSON orders table: timestamp, boolean, double and NULL handling per column.
SELECT id, customer_id, amount, status, created_at, rush,
       date_trunc('month', created_at) AS month,
       amount * 2 AS doubled,
       rush AND amount > 50 AS big_rush,
       length(note) AS note_len
FROM orders
ORDER BY id
