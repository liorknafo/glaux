-- error: ARRAY comparison not supported for arrays with null elements
-- Trino's array ordering operator raises once a shared prefix forces it to
-- read a NULL element. `greatest` used to answer `[a, b]` — and so did
-- `least`, which no total order allows.
SELECT greatest(ARRAY['a', 'b'], ARRAY['a', NULL])
