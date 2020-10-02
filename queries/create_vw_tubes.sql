CREATE OR REPLACE VIEW {serving_dataset}.vw_tubes AS (
SELECT
  tube_assembly_id,
  sum(quantity) as quantity,
  sum(quantity * cost) as total_cost,
FROM
  `tactile-sweep-291117.production.quotes_materials_components`
WHERE
  quote_date BETWEEN "2014-01-01" AND "2017-01-01" 
group by tube_assembly_id, supplier
order by quantity desc
)