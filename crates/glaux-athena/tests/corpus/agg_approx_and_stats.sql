-- Approximate and statistical aggregates.
SELECT approx_distinct(customer_id) AS approx_customers,
       approx_percentile(amount, 0.5) AS median,
       approx_percentile(amount, 0.9) AS p90,
       round(stddev(amount), 4) AS sd,
       round(stddev_samp(amount), 4) AS sd_samp,
       round(stddev_pop(amount), 4) AS sd_pop,
       round(variance(amount), 4) AS var,
       round(var_samp(amount), 4) AS var_s,
       round(var_pop(amount), 4) AS var_p,
       round(corr(amount, customer_id), 4) AS correlation,
       round(covar_samp(amount, customer_id), 4) AS cov_s,
       round(covar_pop(amount, customer_id), 4) AS cov_p
FROM orders
