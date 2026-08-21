-- error: wildcard
SELECT json_extract_scalar(profile, '$.scores[*]') FROM customers
