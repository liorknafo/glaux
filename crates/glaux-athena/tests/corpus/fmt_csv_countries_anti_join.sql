-- NOT EXISTS / NOT IN against the CSV table: countries without customers.
SELECT code, name
FROM countries co
WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.country = co.code)
  AND code NOT IN (SELECT country FROM customers WHERE country IS NOT NULL)
ORDER BY code
