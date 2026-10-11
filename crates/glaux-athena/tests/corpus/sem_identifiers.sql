-- Identifiers are case-insensitive whether quoted or not (Trino rules); output aliases keep their written case.
SELECT ID, Name AS "CustomerName", "country", "ID" AS quoted_upper, C."Country" AS via_alias, length(uuid()) AS uuid_len
FROM "Customers" AS C
WHERE Country = 'US'
ORDER BY id
