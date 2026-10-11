-- substr / split_part / regexp_replace edge cases where DataFusion's own functions differ from Trino.
SELECT substr('hello', -3) AS neg_start, substr('hello', 0, 3) AS zero_start, substr('hello', -3, 2) AS neg_with_len,
       substr('hello', 10) AS past_end, substr('hello', 2, 0) AS zero_len, substr('hello', -10) AS before_start,
       substring('hello' FROM -2) AS syntax_neg, substring('hello', 2, 3) AS comma_form, substring('Überweisung' FROM 1 FOR 4) AS unicode,
       split_part('abc', ',', 2) IS NULL AS missing_is_null, split_part('a,b,,c', ',', 3) = '' AS empty_field,
       split_part('abc', '', 2) AS by_char, split_part('a.b.c', '.', 3) AS last_field,
       regexp_replace('ab', '(a)', '$1x') AS group_then_text, regexp_replace('a.b', '\.', '\$') AS literal_dollar,
       regexp_replace('k=v', '(?<k>\w)=(?<v>\w)', '${v}=${k}') AS named_groups
