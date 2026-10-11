-- error: Column 'a.k' cannot be resolved
SELECT a.k FROM (VALUES (1)) a(k) FULL JOIN (VALUES (1)) b(k) USING (k)
