-- error: lambda expression
SELECT transform(tags, x -> upper(x)) FROM customers
