-- `unknown` survives a set operation between two bare NULLs.
SELECT NULL AS bare_null UNION SELECT NULL
