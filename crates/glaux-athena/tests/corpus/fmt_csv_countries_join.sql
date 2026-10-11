-- customers (Parquet) joined to countries (CSV via LazySimpleSerDe): typed CSV columns survive the join and aggregate.
SELECT co.code, co.name AS country_name, co.continent,
       count(c.id) AS customers,
       co.population, round(co.gdp_per_capita, 1) AS gdp
FROM countries co
LEFT JOIN customers c ON c.country = co.code
GROUP BY co.code, co.name, co.continent, co.population, co.gdp_per_capita
ORDER BY customers DESC, co.code
