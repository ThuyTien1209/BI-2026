CREATE TABLE IF NOT EXISTS `analytics.fact_orders_aggregate` (
    order_id STRING,
    customer_id STRING,
    order_placement_date DATE,
    on_time INT64,
    in_full INT64,
    otif INT64,
    run_id STRING
);

INSERT INTO analytics.fact_orders_aggregate
SELECT
    order_id,
    customer_id,
    order_placement_date,

    CASE
        WHEN MAX(actual_delivery_date) <= MAX(agreed_delivery_date)
        THEN 1 ELSE 0
    END AS on_time,

    CASE
        WHEN SUM(delivered_qty) >= SUM(order_qty)
        THEN 1 ELSE 0
    END AS in_full,

    CASE
        WHEN MAX(actual_delivery_date) <= MAX(agreed_delivery_date)
         AND SUM(delivered_qty) >= SUM(order_qty)
        THEN 1 ELSE 0
    END AS otif,
    '{{ run_id }}' AS run_id

FROM analytics.fact_order_lines
GROUP BY
    order_id,
    customer_id,
    order_placement_date;