-- Typed reads from the Parquet customers table: date, array<varchar> and JSON text columns.
SELECT id, name, country, signup_date,
       year(signup_date) AS signup_year,
       cardinality(tags) AS n_tags,
       element_at(tags, 1) AS first_tag,
       json_extract_scalar(profile, '$.plan') AS plan
FROM customers
ORDER BY id
