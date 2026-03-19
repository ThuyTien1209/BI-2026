{% set clean_run_id = run_id | replace(':', '_') | replace('.', '_') | replace('+', '_') %}

CREATE TABLE `analytics.dim_products_{{ clean_run_id }}` AS
SELECT
  product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  CURRENT_TIMESTAMP() AS load_time,
  '{{ run_id }}' AS run_id
FROM raw.products 
WHERE run_id = '{{ run_id }}';

