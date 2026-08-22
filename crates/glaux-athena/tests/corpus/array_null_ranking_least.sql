-- error: ARRAY comparison not supported for arrays with null elements
SELECT least(ARRAY['a', 'b'], ARRAY['a', NULL])
