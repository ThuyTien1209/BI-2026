{% set clean_run_id = run_id | replace(':', '_') | replace('.', '_') | replace('+', '_') %}

CREATE TABLE `analytics.dim_customers_{{ clean_run_id }}` AS
SELECT
  c.customer_id,
  TRIM(c.customer_name) AS customer_name,
  c.city,
  CURRENT_TIMESTAMP() AS load_time,
  '{{ run_id }}' AS run_id
FROM raw.customer c
WHERE run_id = '{{ run_id }}';