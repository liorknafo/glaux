-- error: For SELECT DISTINCT, ORDER BY expressions must appear in select list
SELECT DISTINCT status FROM orders ORDER BY id
