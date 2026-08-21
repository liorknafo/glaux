-- Aggregates with HAVING, DISTINCT counting, boolean aggregates, and array_agg with ORDER BY.
SELECT status,
       count(*) AS n,
       count(DISTINCT customer_id) AS customers,
       sum(amount) AS total,
       round(avg(amount), 3) AS mean,
       min(amount) AS lo,
       max(amount) AS hi,
       count_if(rush) AS rushed,
       bool_and(rush) AS all_rush,
       bool_or(rush) AS any_rush,
       every(amount > 10) AS all_over_10,
       array_agg(id ORDER BY id) AS ids,
       arbitrary(status) AS any_status
FROM orders
GROUP BY status
HAVING count(*) > 1
ORDER BY status
