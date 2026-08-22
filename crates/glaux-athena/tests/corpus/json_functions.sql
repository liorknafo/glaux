-- JSON accessors over JSON text columns.
SELECT id,
       json_extract_scalar(profile, '$.plan') AS plan,
       json_extract_scalar(profile, '$.age') AS age_text,
       CAST(json_extract_scalar(profile, '$.age') AS INTEGER) AS age,
       json_extract_scalar(profile, '$.address.city') AS city,
       json_extract_scalar(profile, '$["address"]["zip"]') AS zip,
       json_extract_scalar(profile, '$.scores[1]') AS second_score,
       json_extract_scalar(profile, '$.address') AS object_is_null,
       json_extract_scalar(profile, '$.missing') AS missing_is_null,
       json_extract(profile, '$.address') AS address_json,
       json_extract(profile, '$.scores') AS scores_json,
       json_extract_scalar(json_parse(profile), '$.plan') AS via_parse,
       json_format(json_parse('{"b": 1, "a": [true, null]}')) AS formatted,
       json_array_length(json_extract(profile, '$.scores')) AS n_scores,
       json_array_length(profile) AS not_an_array,
       json_size(profile, '$') AS n_keys,
       json_size(profile, '$.scores') AS n_scores2,
       json_size(profile, '$.plan') AS scalar_size
FROM customers
ORDER BY id
