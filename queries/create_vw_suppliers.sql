CREATE OR REPLACE VIEW {serving_dataset}.vw_suppliers AS (
SELECT
 supplier,
 COUNT(*) AS number_of_quotes,
 SUM(quantity) as quantity
FROM
  {production_dataset}.quotes_materials_components
WHERE
  quote_date BETWEEN "2014-01-01" AND "2017-01-01" 
GROUP BY supplier
ORDER BY quantity DESC
)