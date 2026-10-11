-- sqrt of a negative number is NaN (Java's Math.sqrt), not an error.
SELECT sqrt(-1e0) AS neg, sqrt(4) AS int_arg, sqrt(2.25) AS decimal_arg, sqrt(0e0) AS zero, sqrt(NULL) AS null_arg
