CREATE TABLE IF NOT EXISTS analytics.dim_targets_orders (
    order_sk STRING NOT NULL,
    customer_id STRING,
    ontime_target FLOAT64,
    infull_target FLOAT64,
    otif_target FLOAT64,
    run_id STRING,
    load_time TIMESTAMP,
    is_current BOOL,
    PRIMARY KEY (order_sk) NOT ENFORCED
);

MERGE analytics.dim_targets_orders d
USING (
    SELECT DISTINCT t.customer_id
    FROM raw1.target_orders t
    JOIN analytics.dim_targets_orders d2
      ON t.customer_id = d2.customer_id
     AND d2.is_current = TRUE
    WHERE t.run_id = '{{ run_id }}'
      AND (
            d2.ontime_target != t.ontime_target
         OR d2.infull_target != t.infull_target
         OR d2.otif_target   != t.otif_target
      )
) AS changed
ON d.customer_id = changed.customer_id
AND d.is_current = TRUE
WHEN MATCHED THEN
    UPDATE SET is_current = FALSE;

INSERT INTO analytics.dim_targets_orders (order_sk, customer_id, ontime_target, infull_target, otif_target, run_id, load_time, is_current)
SELECT
    GENERATE_UUID() AS order_sk,
    o.customer_id,
    o.ontime_target,
    o.infull_target,
    o.otif_target,
    o.run_id,
    CURRENT_TIMESTAMP(),
    TRUE AS is_current
FROM raw1.target_orders o
LEFT JOIN analytics.dim_targets_orders d
  ON o.customer_id = d.customer_id
 AND d.is_current = TRUE
WHERE o.run_id = '{{ run_id }}'
  AND (
        d.customer_id IS NULL -- new record
     OR d.ontime_target != o.ontime_target
     OR d.infull_target != o.infull_target
     OR d.otif_target != o.otif_target     
   );


