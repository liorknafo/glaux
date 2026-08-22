-- error: POSIX bracket expression `[:alpha:]`
-- Trino runs Joni with Syntax.JAVA, whose operator flags do not include
-- OP_POSIX_BRACKET, so `[[:alpha:]]` there is a class over the characters
-- that spell it and `regexp_like('ABC', '[[:alpha:]]+')` is false. Rust's
-- regex reads the same text as the POSIX class and said true, so glaux
-- refuses the pattern instead of answering with either meaning.
SELECT regexp_like('ABC', '[[:alpha:]]+')
