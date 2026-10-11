-- error: more digits than a Trino decimal holds
-- 2^127 as a bare literal: refused as an integer literal beyond bigint, and
-- the hint points out that DECIMAL '...' will not carry it either.
SELECT 170141183460469231731687303715884105728
