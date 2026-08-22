-- error: ARRAY comparison not supported for arrays with null elements
-- BETWEEN is `>=` AND `<=`, so it refuses what those refuse; it used to
-- answer true while `ARRAY['a','b'] >= ARRAY['a',NULL]` errored.
SELECT ARRAY['a', 'b'] BETWEEN ARRAY['a', NULL] AND ARRAY['z']
