-- error: Cannot cast VARCHAR ' 1.5 ' to DECIMAL(2, 1)
SELECT CAST(' 1.5 ' AS DECIMAL(2,1))
