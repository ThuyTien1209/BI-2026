CREATE OR REPLACE TABLE analytics.fact_orders_aggregate (
    order_id             STRING,
    customer_id          STRING,
    customer_sk          STRING,
    order_placement_date DATE,
    on_time              INT64,
    in_full              INT64,
    otif                 INT64,
    run_id               STRING,
    FOREIGN KEY (customer_sk) REFERENCES analytics.dim_customers(customer_sk) NOT ENFORCED
);

INSERT INTO analytics.fact_orders_aggregate
SELECT
    f.order_id,
    f.customer_id,
    f.customer_sk,
    f.order_placement_date,
    CASE WHEN MAX(f.actual_delivery_date) <= MAX(f.agreed_delivery_date)
         THEN 1 ELSE 0 END AS on_time,
    CASE WHEN SUM(f.delivered_qty) >= SUM(f.order_qty)
         THEN 1 ELSE 0 END AS in_full,
    CASE WHEN MAX(f.actual_delivery_date) <= MAX(f.agreed_delivery_date)
          AND SUM(f.delivered_qty) >= SUM(f.order_qty)
         THEN 1 ELSE 0 END AS otif,
    '{{ run_id }}'
FROM analytics.fact_order_lines f
WHERE f.run_id = '{{ run_id }}'
GROUP BY
    f.order_id,
    f.customer_id,
    f.customer_sk,
    f.order_placement_date;