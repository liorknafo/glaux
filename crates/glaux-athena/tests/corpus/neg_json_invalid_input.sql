-- error: invalid JSON
SELECT json_extract_scalar('{not json', '$.a')
