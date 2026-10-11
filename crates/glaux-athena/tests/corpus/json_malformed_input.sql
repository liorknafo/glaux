-- Text that is not JSON: the accessors return NULL (Trino's varchar overloads); json_parse raises.
SELECT json_extract_scalar('not json', '$.a') AS scalar, json_extract('{not json', '$.a') AS extract,
       json_array_length('[1,2') AS array_length, json_size('{', '$') AS size,
       json_extract_scalar('{"a": 1}', '$.a') AS valid, json_array_length('[1,2]') AS valid_length,
       json_extract_scalar(note, '$.x') AS from_column
FROM orders WHERE id = 107
