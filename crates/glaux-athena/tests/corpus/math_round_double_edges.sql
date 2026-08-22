-- The three edge branches of Trino's MathFunctions.round(double, integer).
-- It is declared `neverFails = true`, so every one of them returns a value.
SELECT round(1e308, 2) AS overflow_product,
       round(-1e308, 2) AS overflow_product_neg,
       round(1e20, 2) AS saturating_round,
       round(1.5e0, 99) AS saturating_round_divides,
       round(1.5e0, -400) AS underflow_factor,
       round(-1.5e0, -400) AS underflow_factor_neg,
       round(0e0, 400) AS zero_times_infinity
