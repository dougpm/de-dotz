WITH type_cast_comp_boss AS (
SELECT
  component_id,
  component_type_id,
  type,
  connection_type_id,
  outside_shape,
  base_type,
  CAST(height_over_tube AS FLOAT64) AS height_over_tube,
  CAST(bolt_pattern_long AS FLOAT64) AS bolt_pattern_long,
  CAST(bolt_pattern_wide AS FLOAT64) AS bolt_pattern_wide,
  groove,
  CAST(base_diameter AS FLOAT64) AS base_diameter,
  CAST(shoulder_diameter AS FLOAT64) AS shoulder_diameter,
  unique_feature,
  orientation,
  CAST(weight AS FLOAT64) AS weight
FROM
  {landing_dataset}.comp_boss

)

SELECT
  price.tube_assembly_id,
  price.supplier,
  CAST(price.quote_date AS DATE) AS quote_date,
  CAST(price.annual_usage AS INT64) AS annual_usage,
  CAST(price.min_order_quantity AS INT64) AS min_order_quantity,
  price.bracket_pricing,
  CAST(price.quantity AS INT64) AS quantity,
  CAST(price.cost AS FLOAT64) AS cost,
  materials.component_id_1,
  CAST(materials.quantity_1 AS INT64) AS quantity_1,
  materials.component_id_2,
  CAST(materials.quantity_2 AS INT64) AS quantity_2,
  materials.component_id_3,
  CAST(materials.quantity_3 AS INT64) AS quantity_3,
  materials.component_id_4,
  CAST(materials.quantity_4 AS INT64) AS quantity_4,
  materials.component_id_5,
  CAST(materials.quantity_5 AS INT64) AS quantity_5,
  materials.component_id_6,
  CAST(materials.quantity_6 AS INT64) AS quantity_6,
  materials.component_id_7,
  CAST(materials.quantity_7 AS INT64) AS quantity_7,
  materials.component_id_8,
  CAST(materials.quantity_8 AS INT64) AS quantity_8,
  comp1.component_type_id AS c1_type_id,
  comp1.type AS c1_type,
  comp1.connection_type_id AS c1_connection_type_id,
  comp1.outside_shape AS c1_outside_shape,
  comp1.base_type AS c1_base_type,
  comp1.height_over_tube AS c1_height_over_tube,
  comp1.bolt_pattern_long AS c1_bolt_pattern_long,
  comp1.bolt_pattern_wide AS c1_bolt_pattern_wide,
  comp1.groove AS c1_groove,
  comp1.base_diameter AS c1_base_diameter,
  comp1.shoulder_diameter AS c1_shoulder_diameter,
  comp1.unique_feature AS c1_unique_feature,
  comp1.orientation AS c1_orientation,
  comp1.weight AS c1_weight,
  
  comp2.component_type_id AS c2_type_id,
  comp2.type AS c2_type,
  comp2.connection_type_id AS c2_connection_type_id,
  comp2.outside_shape AS c2_outside_shape,
  comp2.base_type AS c2_base_type,
  comp2.height_over_tube AS c2_height_over_tube,
  comp2.bolt_pattern_long AS c2_bolt_pattern_long,
  comp2.bolt_pattern_wide AS c2_bolt_pattern_wide,
  comp2.groove AS c2_groove,
  comp2.base_diameter AS c2_base_diameter,
  comp2.shoulder_diameter AS c2_shoulder_diameter,
  comp2.unique_feature AS c2_unique_feature,
  comp2.orientation AS c2_orientation,
  comp2.weight AS c2_weight,
  
  comp3.component_type_id AS c3_type_id,
  comp3.type AS c3_type,
  comp3.connection_type_id AS c3_connection_type_id,
  comp3.outside_shape AS c3_outside_shape,
  comp3.base_type AS c3_base_type,
  comp3.height_over_tube AS c3_height_over_tube,
  comp3.bolt_pattern_long AS c3_bolt_pattern_long,
  comp3.bolt_pattern_wide AS c3_bolt_pattern_wide,
  comp3.groove AS c3_groove,
  comp3.base_diameter AS c3_base_diameter,
  comp3.shoulder_diameter AS c3_shoulder_diameter,
  comp3.unique_feature AS c3_unique_feature,
  comp3.orientation AS c3_orientation,
  comp3.weight AS c3_weight,
  
  comp4.component_type_id AS c4_type_id,
  comp4.type AS c4_type,
  comp4.connection_type_id AS c4_connection_type_id,
  comp4.outside_shape AS c4_outside_shape,
  comp4.base_type AS c4_base_type,
  comp4.height_over_tube AS c4_height_over_tube,
  comp4.bolt_pattern_long AS c4_bolt_pattern_long,
  comp4.bolt_pattern_wide AS c4_bolt_pattern_wide,
  comp4.groove AS c4_groove,
  comp4.base_diameter AS c4_base_diameter,
  comp4.shoulder_diameter AS c4_shoulder_diameter,
  comp4.unique_feature AS c4_unique_feature,
  comp4.orientation AS c4_orientation,
  comp4.weight AS c4_weight,
  
  comp5.component_type_id AS c5_type_id,
  comp5.type AS c5_type,
  comp5.connection_type_id AS c5_connection_type_id,
  comp5.outside_shape AS c5_outside_shape,
  comp5.base_type AS c5_base_type,
  comp5.height_over_tube AS c5_height_over_tube,
  comp5.bolt_pattern_long AS c5_bolt_pattern_long,
  comp5.bolt_pattern_wide AS c5_bolt_pattern_wide,
  comp5.groove AS c5_groove,
  comp5.base_diameter AS c5_base_diameter,
  comp5.shoulder_diameter AS c5_shoulder_diameter,
  comp5.unique_feature AS c5_unique_feature,
  comp5.orientation AS c5_orientation,
  comp5.weight AS c5_weight,
  
  comp6.component_type_id AS c6_type_id,
  comp6.type AS c6_type,
  comp6.connection_type_id AS c6_connection_type_id,
  comp6.outside_shape AS c6_outside_shape,
  comp6.base_type AS c6_base_type,
  comp6.height_over_tube AS c6_height_over_tube,
  comp6.bolt_pattern_long AS c6_bolt_pattern_long,
  comp6.bolt_pattern_wide AS c6_bolt_pattern_wide,
  comp6.groove AS c6_groove,
  comp6.base_diameter AS c6_base_diameter,
  comp6.shoulder_diameter AS c6_shoulder_diameter,
  comp6.unique_feature AS c6_unique_feature,
  comp6.orientation AS c6_orientation,
  comp6.weight AS c6_weight,
  
  comp7.component_type_id AS c7_type_id,
  comp7.type AS c7_type,
  comp7.connection_type_id AS c7_connection_type_id,
  comp7.outside_shape AS c7_outside_shape,
  comp7.base_type AS c7_base_type,
  comp7.height_over_tube AS c7_height_over_tube,
  comp7.bolt_pattern_long AS c7_bolt_pattern_long,
  comp7.bolt_pattern_wide AS c7_bolt_pattern_wide,
  comp7.groove AS c7_groove,
  comp7.base_diameter AS c7_base_diameter,
  comp7.shoulder_diameter AS c7_shoulder_diameter,
  comp7.unique_feature AS c7_unique_feature,
  comp7.orientation AS c7_orientation,
  comp7.weight AS c7_weight,
  
  comp8.component_type_id AS c8_type_id,
  comp8.type AS c8_type,
  comp8.connection_type_id AS c8_connection_type_id,
  comp8.outside_shape AS c8_outside_shape,
  comp8.base_type AS c8_base_type,
  comp8.height_over_tube AS c8_height_over_tube,
  comp8.bolt_pattern_long AS c8_bolt_pattern_long,
  comp8.bolt_pattern_wide AS c8_bolt_pattern_wide,
  comp8.groove AS c8_groove,
  comp8.base_diameter AS c8_base_diameter,
  comp8.shoulder_diameter AS c8_shoulder_diameter,
  comp8.unique_feature AS c8_unique_feature,
  comp8.orientation AS c8_orientation,
  comp8.weight AS c8_weight,
FROM
  {landing_dataset}.price_quote price
LEFT JOIN
  {landing_dataset}.bill_of_materials materials
USING
  (tube_assembly_id)
LEFT JOIN
  type_cast_comp_boss comp1
ON
  component_id_1 = comp1.component_id
LEFT JOIN
  type_cast_comp_boss comp2
ON
  component_id_2 = comp2.component_id
LEFT JOIN
  type_cast_comp_boss comp3
ON
  component_id_3 = comp3.component_id
LEFT JOIN
  type_cast_comp_boss comp4
ON
  component_id_4 = comp4.component_id
LEFT JOIN
  type_cast_comp_boss comp5
ON
  component_id_5 = comp5.component_id
LEFT JOIN
  type_cast_comp_boss comp6
ON
  component_id_6 = comp6.component_id
LEFT JOIN
  type_cast_comp_boss comp7
ON
  component_id_7 = comp7.component_id
LEFT JOIN
  type_cast_comp_boss comp8
ON
  component_id_8 = comp8.component_id
