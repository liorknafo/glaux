-- Integer literals are INTEGER (32-bit) as on Trino/Athena; arithmetic stays integer and widens only when mixed with bigint.
SELECT 1 AS one, 1 + 1 AS two, 7 / 2 AS int_div, 7 % 2 AS int_mod, -1 AS negative, 2147483647 AS max_int, 2147483648 AS beyond_int,
       -2147483648 AS min_int, id + 1 AS bigint_plus_int, count(*) + 1 AS count_plus, sum(1) AS sum_of_ints, max(1) AS max_int_literal,
       CAST(2147483647 AS INTEGER) AS cast_int, CAST(1 AS INTEGER) + CAST(2 AS INTEGER) AS int_plus_int,
       abs(-5) AS abs_int, coalesce(customer_id, 0) AS coalesced, CASE WHEN amount > 50 THEN 1 ELSE 0 END AS case_int,
       x AS from_values, id IN (101, 102) AS in_list
FROM orders, (VALUES (1), (2)) t(x)
WHERE id = 101
GROUP BY id, customer_id, amount, x
ORDER BY x
