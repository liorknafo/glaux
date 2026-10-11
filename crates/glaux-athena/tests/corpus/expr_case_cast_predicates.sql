-- CASE (simple and searched), CAST / TRY_CAST, BETWEEN, IN, LIKE, IS DISTINCT FROM, if(), coalesce(), nullif().
SELECT id,
       CASE status WHEN 'shipped' THEN 'done' WHEN 'pending' THEN 'open' ELSE 'other' END AS simple_case,
       CASE WHEN amount >= 100 THEN 'large' WHEN amount >= 30 THEN 'medium' ELSE 'small' END AS size,
       CAST(amount AS BIGINT) AS amount_int,
       CAST(id AS VARCHAR) || '-' || coalesce(status, 'none') AS label,
       TRY_CAST(note AS BIGINT) AS note_as_int,
       TRY_CAST(split_part(note, '=', 2) AS INTEGER) AS parsed,
       amount BETWEEN 30 AND 100 AS mid,
       status IN ('shipped', 'pending') AS active,
       note LIKE '%ABC%' AS has_abc,
       status IS DISTINCT FROM 'shipped' AS not_shipped,
       if(rush, 'rush', 'standard') AS priority,
       if(amount > 200, 'big') AS big_only,
       nullif(status, 'pending') AS status_or_null,
       CAST(1.5 AS DOUBLE) + CAST('2' AS INTEGER) AS arith
FROM orders
ORDER BY id
