CREATE TABLE IF NOT EXISTS `analytics.fact_order_lines` (
    order_line_id INT64,
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    order_placement_date DATE,
    order_qty INT64,
    delivered_qty INT64,
    agreed_delivery_date DATE,
    actual_delivery_date DATE
)

INSERT INTO `analytics.fact_order_lines`
SELECT
    ol.order_line_id,
    o.order_id,
    o.customer_id,
    ol.product_id,
    DATE(o.order_placement_date) AS order_placement_date,
    ol.order_qty,
    ol.delivery_qty AS delivered_qty,
    DATE(ol.agreed_delivery_date) AS agreed_delivery_date,
    DATE(ol.actual_delivery_date) AS actual_delivery_date
FROM `raw.orders` o
JOIN `raw.order_lines` ol
ON o.order_id = ol.order_id
WHERE NOT EXISTS (
    SELECT 1
    FROM `analytics.fact_order_lines` f
    WHERE f.order_line_id = ol.order_line_id
);