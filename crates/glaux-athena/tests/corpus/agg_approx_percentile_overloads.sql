-- approx_percentile over Trino's bigint / real / double overloads, with a
-- tiny input so the t-digest never merges and the answer is exactly Trino's
-- (bigint results are Math.round-ed; the nulls in amount are skipped).
SELECT approx_percentile(id, 0.5) AS id_median,
       approx_percentile(id, 0.0) AS id_min,
       approx_percentile(id, 1.0) AS id_max,
       approx_percentile(CAST(amount AS REAL), 0.9) AS amount_real_p90,
       approx_percentile(amount, 0.25) AS amount_p25,
       approx_percentile(amount, 0.75) AS amount_p75
FROM orders
