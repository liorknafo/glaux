-- Trino's varchar -> integral casts call Long.parseLong on the text as it stands (Athena engine v3 / Trino 411: `Long.parseLong(slice.toStringUtf8())`), so surrounding whitespace is a cast failure — as it already is for DECIMAL and BOOLEAN. Only DOUBLE / REAL trim, following Double.parseDouble.
-- error: Cannot cast '  12  ' to bigint
SELECT CAST('  12  ' AS BIGINT)
