-- error: Invalid numeric literal: 12345678901234567890
-- Trino's grammar has one production for a digits-only literal
-- (`number : MINUS? INTEGER_VALUE #integerLiteral`); AstBuilder turns it into
-- a LongLiteral, whose constructor parses with Long.parseLong and raises
-- `Invalid numeric literal` when it does not fit. There is no path from a
-- digits-only token to a DecimalLiteral (that production needs a decimal
-- point), and glaux relies on the same rule for its `-9223372036854775808`
-- → bigint fold. glaux used to answer decimal(20,0) here — a value where
-- Athena refuses the query.
SELECT 12345678901234567890
