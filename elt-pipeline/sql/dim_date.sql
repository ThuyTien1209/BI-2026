CREATE OR REPLACE TABLE `analytics.dim_date` AS
WITH date_spine AS (
    SELECT date
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            (SELECT MIN(order_placement_date) FROM `raw.orders`),
            (SELECT MAX(actual_delivery_date) FROM `raw.order_lines`)
        )
    ) AS date
)

SELECT
    date,
    FORMAT_DATE('%b-%y', date)  AS mmm_yy,
    EXTRACT(ISOWEEK FROM date)  AS week_no
FROM date_spine