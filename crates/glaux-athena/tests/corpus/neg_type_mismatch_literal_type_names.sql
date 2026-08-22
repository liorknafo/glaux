-- error: Cannot apply operator: integer = varchar(1)
-- Trino types the literals `integer` and `varchar(1)`; the diagnostic used to
-- report DataFusion's planned `bigint` and unbounded `varchar`, contradicting
-- glaux's own `SELECT 1` → integer.
SELECT 1 = '1'
