-- array_agg with an ORDER BY argument: NULL elements sort last in both directions, as in Trino.
SELECT array_agg(amount ORDER BY amount DESC) AS amounts_desc,
       array_agg(amount ORDER BY amount) AS amounts_asc,
       array_agg(id ORDER BY amount DESC, id) AS ids_by_amount_desc
FROM orders
