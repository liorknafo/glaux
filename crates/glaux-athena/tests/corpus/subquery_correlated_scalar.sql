-- A correlated scalar subquery that is not itself an aggregate: valid
-- Trino, one value per outer row and NULL when nothing matches. DataFusion
-- refused the shape ("Correlated scalar subquery must be aggregated to
-- return at most one row"), so it is wrapped in trino_single_value /
-- trino_group_rows with the row check outside the subquery.
SELECT o.id,
       (SELECT c.name FROM customers c WHERE c.id = o.customer_id) AS cname,
       (SELECT c.tags FROM customers c WHERE c.id = o.customer_id) AS ctags,
       (SELECT upper(c.country) FROM customers c WHERE c.id = o.customer_id) AS ccountry,
       (SELECT c.name FROM customers c WHERE c.id = o.customer_id) || '!' AS used_in_expr,
       -- Several inner rows per correlation group, but no outer row selects
       -- any of them: Trino raises SUBQUERY_MULTIPLE_ROWS only for a group
       -- an outer row actually matches, so this is NULL, not an error.
       (SELECT c.name FROM customers c WHERE c.country = o.status) AS no_outer_match,
       -- The aggregated form keeps working.
       (SELECT count(*) FROM customers c WHERE c.id = o.customer_id) AS n
FROM orders o
ORDER BY o.id
