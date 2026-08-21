-- Unquoted identifiers fold to lower case; quoted ones keep their case and can alias output columns.
SELECT ID, Name AS "CustomerName", "country", length(uuid()) AS uuid_len
FROM CUSTOMERS
WHERE Country = 'US'
ORDER BY id
