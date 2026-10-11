-- error: ARRAY comparison not supported for arrays with null elements
-- `array_sort` ranks the elements, so an element that is itself an array
-- with a NULL inside is refused; a plain NULL element still sorts last.
SELECT array_sort(ARRAY[ARRAY['a', 'b'], ARRAY['a', NULL]])
