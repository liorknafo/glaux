-- Trino's float constructors. glaux has no NaN or infinite literal to fold
-- onto, so they translate to its Java-shaped varchar -> double cast; every
-- rule the coverage table documents with nan() is runnable through them.
SELECT nan() AS not_a_number,
       infinity() AS positive_infinity,
       -infinity() AS negative_infinity,
       CAST(nan() AS VARCHAR) AS nan_text,
       CAST(infinity() AS VARCHAR) AS infinity_text,
       nan() = nan() AS nan_never_equals,
       nan() <> nan() AS nan_always_differs,
       power(1, nan()) AS power_of_one,
       nullif(nan(), nan()) AS nullif_keeps_nan,
       greatest(1e0, nan()) AS greatest_ranks_nan_smallest,
       least(1e0, nan()) AS least_ranks_nan_smallest,
       array_sort(ARRAY[nan(), 1e0, 2e0]) AS sorted,
       infinity() > 1e300 AS infinity_is_largest,
       1e0 / 0e0 = infinity() AS matches_the_division_form
