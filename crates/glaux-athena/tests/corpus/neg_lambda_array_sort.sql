-- error: lambda
SELECT array_sort(tags, (a, b) -> 1) FROM customers
