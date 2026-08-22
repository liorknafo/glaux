-- JSON-typed expressions report Athena's `json` column type, not the `varchar` glaux stores the text as; json_format / json_extract_scalar are varchar on Athena too.
SELECT json_parse('{"b": 1, "a": 2}') AS parsed,
       json_extract(profile, '$.address') AS extracted,
       json_extract_scalar(profile, '$.plan') AS scalar_text,
       json_format(json_parse('{"a": 1}')) AS formatted,
       json_size(profile, '$.address') AS size
FROM customers WHERE id = 1
