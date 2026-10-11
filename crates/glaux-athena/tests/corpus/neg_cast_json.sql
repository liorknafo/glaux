-- error: CAST(... AS JSON)
-- Trino's CAST(x AS JSON) builds the JSON *value* of x, so a varchar source
-- becomes a quoted JSON string; glaux carries JSON as its text, so returning
-- the text would answer differently from Athena. json_parse / json_format do
-- what this cast is usually reached for.
SELECT CAST(profile AS JSON) FROM customers WHERE id = 1
