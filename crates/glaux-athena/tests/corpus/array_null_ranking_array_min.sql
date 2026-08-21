-- error: ARRAY comparison not supported for arrays with null elements
SELECT array_min(ARRAY[ARRAY['a', 'b'], ARRAY['a', NULL]])
