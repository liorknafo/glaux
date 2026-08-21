-- A computed (non-literal) LIKE pattern keeps Trino's no-default-escape
-- semantics: backslashes are literal (the TrinoSemantics analyzer doubles
-- them for DataFusion's escape-aware matcher at run time).
SELECT 'a\bc' LIKE substr('a\_cXX', 1, 4) AS backslash_then_wildcard,
       'a_c' LIKE substr('a\_cXX', 1, 4) AS backslash_literal_no_match,
       'abc' LIKE upper('a%') AS upper_pattern_no_match,
       'ABC' LIKE upper('a%') AS upper_pattern_match,
       note LIKE note AS self_match
FROM orders WHERE id = 101
