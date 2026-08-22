-- error: column 1 in UNION query has incompatible types: varchar(1), integer
-- The literals are named with the types Trino gives them, as they already
-- are in ARRAY[...] and VALUES; this path still reported DataFusion's
-- `varchar` and `bigint`.
SELECT 'a' UNION ALL SELECT 1
