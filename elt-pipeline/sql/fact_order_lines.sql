CREATE TABLE IF NOT EXISTS `analytics.fact_order_lines` (
    order_line_id STRING,
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    order_placement_date DATE,
    order_qty INT64,
    delivered_qty INT64,
    agreed_delivery_date DATE,
    actual_delivery_date DATE,
    revenue FLOAT64,
    expense FLOAT64,
    load_time TIMESTAMP,
    run_id STRING
);

INSERT INTO `analytics.fact_order_lines` (
    order_line_id,
    order_id,
    customer_id,
    product_id,
    order_placement_date,
    order_qty,
    delivered_qty,
    agreed_delivery_date,
    actual_delivery_date,
    revenue,
    expense,
    load_time,
    run_id
)

SELECT
    ol.order_line_id,
    o.order_id,
    o.customer_id,
    ol.product_id,
    DATE(o.order_placement_date) AS order_placement_date,
    ol.order_qty,
    ol.delivery_qty AS delivered_qty,
    DATE(ol.agreed_delivery_date) AS agreed_delivery_date,
    DATE(ol.actual_delivery_date) AS actual_delivery_date,
    ol.delivery_qty*p.price*1.0 AS revenue,
    ol.delivery_qty*p.cost*1.0 AS expense,
    CURRENT_TIMESTAMP(),
    '{{ run_id }}'
FROM `raw.orders` o
JOIN `raw.order_lines` ol
ON o.order_id = ol.order_id
JOIN `raw.products` p
ON p.product_id = ol.product_id
WHERE o.run_id = '{{ run_id }}'
  AND ol.run_id = '{{ run_id }}' AND p.run_id = '{{ run_id }}';