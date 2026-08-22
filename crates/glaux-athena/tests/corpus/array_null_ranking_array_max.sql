-- error: ARRAY comparison not supported for arrays with null elements
-- Ranking arrays-of-arrays: `array_max` / `array_min` used to answer
-- `[a, b]` for both. (A NULL *element* is still NULL, not an error — see
-- array_null_elements.)
SELECT array_max(ARRAY[ARRAY['a', 'b'], ARRAY['a', NULL]])
