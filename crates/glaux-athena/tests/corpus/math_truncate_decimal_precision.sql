-- Trino's one-argument truncate(decimal(p, s)) is decimal(max(1, p - s), 0)
-- (MathFunctions.Truncate's `@Constraint(variable = "rp", expression =
-- "max(1, p - s)")`), one digit narrower than ceiling / floor's
-- `p - s + min(s, 1)`, which glaux used to reuse. The two-argument form
-- keeps decimal(p, s).
SELECT truncate(1.98) AS t_1_98,
       truncate(9.9) AS t_9_9,
       truncate(123.45) AS t_123_45,
       truncate(CAST(0.9 AS DECIMAL(1,1))) AS t_scale_only,
       truncate(CAST(12345 AS DECIMAL(5,0))) AS t_no_scale,
       truncate(1.98, 1) AS t_two_args,
       ceil(1.98) AS ceil_1_98,
       floor(1.98) AS floor_1_98,
       round(1.98) AS round_1_98
