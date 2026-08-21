-- Math functions and the renamed ones (mod, sign, truncate, rand).
SELECT abs(-3.5) AS a, ceil(1.2) AS c, ceiling(1.2) AS c2, floor(-1.2) AS f,
       round(2.567, 2) AS r2, round(2.5) AS r0,
       sqrt(16.0) AS sq, cbrt(27.0) AS cb, exp(0) AS e0, ln(1) AS l1,
       log(2, 8) AS log2_8, log10(1000) AS l10, log2(8) AS l2,
       mod(10, 3) AS m, mod(-7, 3) AS m_neg,
       pi() > 3.14 AS pi_ok, pow(2, 10) AS p, power(2, 0.5) AS p2,
       sign(-2.5) AS s_neg, sign(0) AS s_zero, sign(7) AS s_pos,
       truncate(2.7) AS t, truncate(-2.7) AS t_neg,
       greatest(1, 5, 3) AS g, least(1, 5, 3) AS l,
       random() < 2 AS rnd_ok, rand() >= 0 AS rand_ok
