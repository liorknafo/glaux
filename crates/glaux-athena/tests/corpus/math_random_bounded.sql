-- Trino's bounded random(n) overload: a uniform value in [0, n) with n's own
-- integer type. (The value is random, so the case asserts the range and the
-- type rather than a draw.)
SELECT random(5) >= 0 AND random(5) < 5 AS int_in_range,
       random(CAST(5 AS BIGINT)) >= 0 AND random(CAST(5 AS BIGINT)) < 5 AS bigint_in_range,
       random() >= 0e0 AND random() < 1e0 AS nullary_in_range,
       random(1) AS only_zero
