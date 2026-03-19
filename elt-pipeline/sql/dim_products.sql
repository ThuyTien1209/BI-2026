CREATE TABLE IF NOT EXISTS `analytics.dim_products` (
    product_id    STRING,
    product_name  STRING,
    category      STRING,
)

MERGE `analytics.dim_products` T
USING (
    SELECT
        product_id,
        product_name,
        category
    FROM `raw.products`
) S
ON T.product_id = S.product_id

WHEN MATCHED THEN
UPDATE SET
    product_name = S.product_name,
    category = S.category

WHEN NOT MATCHED THEN
INSERT (product_id, product_name, category)
VALUES (S.product_id, S.product_name, S.category);

