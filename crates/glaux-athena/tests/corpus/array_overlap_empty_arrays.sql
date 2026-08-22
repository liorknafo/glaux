-- Trino's ArraysOverlapFunction returns false for an empty array before
-- it inspects NULL elements, so an empty array against ARRAY[NULL] is
-- false, while ARRAY[NULL] against ARRAY[NULL] is NULL.
SELECT
  arrays_overlap(ARRAY[], ARRAY[NULL]) AS empty_vs_null,
  arrays_overlap(ARRAY[NULL], ARRAY[]) AS null_vs_empty,
  arrays_overlap(ARRAY[], ARRAY[]) AS empty_vs_empty,
  arrays_overlap(ARRAY[NULL], ARRAY[NULL]) AS null_vs_null,
  arrays_overlap(ARRAY[1], ARRAY[NULL]) AS one_vs_null,
  arrays_overlap(ARRAY[1, NULL], ARRAY[1, 3]) AS match_wins
