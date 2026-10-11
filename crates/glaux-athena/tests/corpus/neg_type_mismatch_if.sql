-- error: All CASE results must be the same type
SELECT if(id = 1, 1, 'a') FROM customers
