-- error: UNNEST
SELECT t.tag FROM customers CROSS JOIN UNNEST(tags) AS t(tag)
