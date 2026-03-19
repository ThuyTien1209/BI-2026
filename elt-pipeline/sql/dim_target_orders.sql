CREATE TABLE IF NOT EXISTS analytics.dim_customer_targets (
    customer_id STRING,
    ontime_target FLOAT,
    infull_target FLOAT,
    otif_target FLOAT
);

MERGE analytics.dim_customer_targets T
USING (
    SELECT
        customer_id,
        ontime_target,
        infull_target,
        otif_target
    FROM raw.target_orders
) S
ON T.customer_id = S.customer_id

WHEN MATCHED THEN
UPDATE SET
    ontime_target = S.ontime_target,
    infull_target = S.infull_target,
    otif_target = S.otif_target

WHEN NOT MATCHED THEN
INSERT (
    customer_id,
    ontime_target,
    infull_target,
    otif_target
)
VALUES (
    S.customer_id,
    S.ontime_target,
    S.infull_target,
    S.otif_target
);