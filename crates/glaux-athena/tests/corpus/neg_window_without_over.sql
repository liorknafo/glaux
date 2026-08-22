-- error: rank is a window function and requires an OVER clause
-- DataFusion's own text is `Invalid function 'rank'. Did you mean 'rand'?`.
SELECT rank() FROM orders
