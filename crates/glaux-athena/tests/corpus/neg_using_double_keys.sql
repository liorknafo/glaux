-- error: JOIN ... USING on DOUBLE / REAL keys
SELECT * FROM (VALUES (1e0)) a(k) JOIN (VALUES (1e0)) b(k) USING (k)
