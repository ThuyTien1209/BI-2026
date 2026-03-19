CREATE TABLE IF NOT EXISTS `analytics.dim_customers` (
    customer_id   STRING,
    customer_name STRING,
    city          STRING,
)

MERGE `analytics.dim_customers` T
USING (
    SELECT
        id AS customer_id,
        customer_name,
        city
    FROM `raw.customers`
) S
ON T.customer_id = S.customer_id

WHEN MATCHED THEN
UPDATE SET
    customer_name = S.customer_name,
    city = S.city

WHEN NOT MATCHED THEN
INSERT (customer_id, customer_name, city)
VALUES (S.customer_id, S.customer_name, S.city);