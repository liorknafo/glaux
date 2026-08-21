-- Ranking, offset, and frame-based window functions.
SELECT id, customer_id, amount,
       row_number() OVER (PARTITION BY customer_id ORDER BY id) AS rn,
       rank() OVER (ORDER BY amount DESC NULLS LAST) AS rnk,
       dense_rank() OVER (ORDER BY status) AS drnk,
       lag(amount, 1) OVER (PARTITION BY customer_id ORDER BY id) AS prev_amount,
       lead(id) OVER (ORDER BY id) AS next_id,
       sum(amount) OVER (PARTITION BY customer_id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running,
       first_value(id) OVER (PARTITION BY customer_id ORDER BY id) AS first_id,
       last_value(id) OVER (PARTITION BY customer_id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_id,
       nth_value(id, 2) OVER (PARTITION BY customer_id ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_id,
       ntile(2) OVER (ORDER BY id) AS bucket,
       round(percent_rank() OVER (ORDER BY id), 2) AS pr,
       round(cume_dist() OVER (ORDER BY id), 3) AS cd,
       count_if(rush) OVER (PARTITION BY customer_id) AS rush_in_group
FROM orders
WHERE customer_id IS NOT NULL
ORDER BY id
