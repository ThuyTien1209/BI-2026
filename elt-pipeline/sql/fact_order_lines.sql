CREATE OR REPLACE TABLE analytics.fact_order_lines (
    fact_sk              STRING NOT NULL,
    order_line_id        STRING,
    order_id             STRING,
    customer_id          STRING,
    product_id           STRING,
    order_sk             STRING,
    customer_sk          STRING,
    product_sk           STRING,
    order_placement_date DATE,
    order_qty            INT64,
    delivered_qty        INT64,
    agreed_delivery_date DATE,
    actual_delivery_date DATE,
    on_time              INT64,
    in_full              INT64,
    otif                 INT64,
    actual_revenue       FLOAT64,
    expected_revenue     FLOAT64,
    expense              FLOAT64,
    load_time            TIMESTAMP,
    run_id               STRING,
    PRIMARY KEY (fact_sk) NOT ENFORCED,
    FOREIGN KEY (order_sk)    REFERENCES analytics.dim_targets_orders(order_sk) NOT ENFORCED,
    FOREIGN KEY (customer_sk) REFERENCES analytics.dim_customers(customer_sk)   NOT ENFORCED,
    FOREIGN KEY (product_sk)  REFERENCES analytics.dim_products(product_sk)     NOT ENFORCED
);

INSERT INTO analytics.fact_order_lines (
    fact_sk,
    order_line_id,
    order_id,
    customer_id,
    product_id,
    order_sk,
    customer_sk,
    product_sk,
    order_placement_date,
    order_qty,
    delivered_qty,
    agreed_delivery_date,
    actual_delivery_date,
    on_time,
    in_full,
    otif,
    actual_revenue,
    expected_revenue,
    expense,
    load_time,
    run_id
)
WITH base AS (
    SELECT
        GENERATE_UUID()                  AS fact_sk,
        ol.order_line_id,
        ro.order_id,
        ro.customer_id,
        ol.product_id,
        dto.order_sk,
        dc.customer_sk,
        dp.product_sk,
        DATE(ro.order_placement_date)    AS order_placement_date,
        ol.order_qty,
        ol.delivery_qty                  AS delivered_qty,
        DATE(ol.agreed_delivery_date)    AS agreed_delivery_date,
        DATE(ol.actual_delivery_date)    AS actual_delivery_date,
        CASE
            WHEN ol.agreed_delivery_date IS NULL THEN NULL
            WHEN DATE(ol.actual_delivery_date) <= DATE(ol.agreed_delivery_date) THEN 1
            ELSE 0
        END                              AS on_time,
        CASE
            WHEN ol.delivery_qty >= ol.order_qty THEN 1
            ELSE 0
        END                              AS in_full,
        ol.delivery_qty * dp.price       AS actual_revenue,
        ol.order_qty    * dp.price       AS expected_revenue,
        ol.delivery_qty * dp.cost        AS expense,
        CURRENT_TIMESTAMP()              AS load_time,
        '{{ run_id }}'                   AS run_id
    FROM raw1.orders ro
    JOIN raw1.order_lines ol
      ON ro.order_id = ol.order_id
    JOIN analytics.dim_targets_orders dto
      ON ro.customer_id = dto.customer_id
     AND dto.is_current = TRUE
    JOIN analytics.dim_customers dc
      ON ro.customer_id = dc.customer_id
     AND dc.is_current = TRUE
    JOIN analytics.dim_products dp
      ON ol.product_id = dp.product_id
     AND dp.is_current = TRUE
    WHERE ro.run_id = '{{ run_id }}'
      AND ol.run_id = '{{ run_id }}'
)

SELECT
    fact_sk,
    order_line_id,
    order_id,
    customer_id,
    product_id,
    order_sk,
    customer_sk,
    product_sk,
    order_placement_date,
    order_qty,
    delivered_qty,
    agreed_delivery_date,
    actual_delivery_date,
    on_time,
    in_full,
    CASE WHEN on_time = 1 AND in_full = 1 THEN 1 ELSE 0 END AS otif,
    actual_revenue,
    expected_revenue,
    expense,
    load_time,
    run_id
FROM base;