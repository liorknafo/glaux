-- error: possessive quantifier
SELECT regexp_like('aaa', 'a{1,2}+a')
