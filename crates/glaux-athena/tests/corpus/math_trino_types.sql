-- round / floor / ceil / truncate / sign keep Trino's result types and rounding rules.
SELECT round(2.675e0, 2) AS round_exact_binary, round(1.115e0, 2) AS round_exact_binary2, round(-2.675e0, 2) AS round_neg,
       round(0.285e0, 2) AS round_28, round(1.005e0, 2) AS round_100, round(2.5e0) AS round_half_double, round(-2.5e0) AS round_neg_half,
       round(2.5) AS round_dec, round(2.789, 2) AS round_dec_2, round(5) AS round_int, round(1250, -2) AS round_int_neg, round(CAST(1.5 AS REAL)) AS round_real,
       floor(5) AS floor_int, ceil(5) AS ceil_int, ceiling(CAST(5 AS INTEGER)) AS ceiling_int, truncate(5) AS truncate_int, sign(-2) AS sign_int,
       floor(2.5) AS floor_dec, ceil(2.5) AS ceil_dec, floor(-2.5) AS floor_neg_dec, ceil(-2.5) AS ceil_neg_dec, truncate(2.789, 2) AS truncate_dec_2,
       truncate(2.7) AS truncate_dec, sign(2.5) AS sign_dec, sign(-0.01) AS sign_neg_dec, sign(0.0) AS sign_zero_dec,
       floor(2.5e0) AS floor_double, ceil(-2.5e0) AS ceil_double, round(0.015e0, 2) AS round_015, round(0.49999999999999994e0) AS round_below_half, round(1e20, 2) AS round_huge,
       truncate(-2.7e0) AS truncate_double, sign(-0.5e0) AS sign_double, sign(0e0) AS sign_zero_double,
       least(1.5, 2e0) AS least_mixed, greatest(1, 2.5e0) AS greatest_mixed, coalesce(NULL, 1.5, 2e0) AS coalesce_mixed, CASE WHEN 1 = 1 THEN 1.5 ELSE 2e0 END AS case_mixed, 1.5 + 2e0 AS dec_plus_double, 1.5 = 1.5e0 AS dec_equals_double, nullif(1.5, 1.5e0) AS nullif_mixed, greatest(1.5, 2.25) AS greatest_dec, least(1, 2) AS least_int,
       power(2, 10) AS power_int, pow(2.0, 3) AS pow_dec, mod(7, 2) AS mod_int, abs(-2.5) AS abs_dec
