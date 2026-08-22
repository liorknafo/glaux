-- error: ARRAY comparison not supported for arrays with NaN elements
SELECT ARRAY[CAST('NaN' AS DOUBLE)] < ARRAY[1e0]
