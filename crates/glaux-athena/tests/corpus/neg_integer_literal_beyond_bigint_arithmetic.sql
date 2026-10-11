-- error: Invalid numeric literal: 9223372036854775808
-- Reachable from any expression, not just a bare SELECT.
SELECT 9223372036854775808 + 1
