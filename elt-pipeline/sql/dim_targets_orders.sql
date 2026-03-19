{% set clean_run_id = run_id | replace(':', '_') | replace('.', '_') | replace('+', '_') %}

CREATE TABLE `analytics.dim_targets_orders_{{ clean_run_id }}` AS
SELECT
  customer_id,
  SAFE_CAST(ontime_target AS FLOAT64) AS ontime_target,
  SAFE_CAST(infull_target AS FLOAT64) AS infull_target,
  SAFE_CAST(otif_target AS FLOAT64) AS otif_target,
  CURRENT_TIMESTAMP() AS load_time,
  '{{ run_id }}' AS run_id
FROM raw.target_orders
WHERE run_id = '{{ run_id }}';
