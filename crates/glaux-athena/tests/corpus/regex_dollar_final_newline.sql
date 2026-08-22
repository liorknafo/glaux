-- Joni's single-line `$` (and `\Z`) is a zero-width assertion: it holds at the end of the text or before a final newline without consuming it, so the newline survives regexp_replace and stays out of regexp_extract.
SELECT replace(regexp_extract('ab' || chr(10), 'b$'), chr(10), '<NL>') AS extract_dollar,
       length(regexp_extract('ab' || chr(10), 'b$')) AS extract_dollar_length,
       replace(regexp_extract('ab' || chr(10), 'b\Z'), chr(10), '<NL>') AS extract_big_z,
       replace(regexp_extract('ab' || chr(10), '(b)$', 1), chr(10), '<NL>') AS extract_group,
       replace(regexp_replace('ab' || chr(10), 'b$', 'x'), chr(10), '<NL>') AS replace_dollar,
       length(regexp_replace('ab' || chr(10), 'b$', 'x')) AS replace_dollar_length,
       replace(regexp_replace('ab' || chr(10), 'b\Z', 'x'), chr(10), '<NL>') AS replace_big_z,
       replace(regexp_replace('ab', 'b$', 'x'), chr(10), '<NL>') AS replace_no_newline,
       replace(regexp_replace('ab' || chr(10) || 'cb' || chr(10), '(?m)b$', 'X'), chr(10), '<NL>') AS replace_multiline,
       regexp_like('ab' || chr(10), 'b$') AS like_dollar,
       regexp_like('ab' || chr(10), 'b\z') AS like_lower_z,
       replace(regexp_extract('ab' || chr(10), 'b\z'), chr(10), '<NL>') AS extract_lower_z,
       regexp_extract(note, '^\s*(\S+)\s*$', 1) AS trimmed_note
FROM (SELECT '  padded  ' AS note)
