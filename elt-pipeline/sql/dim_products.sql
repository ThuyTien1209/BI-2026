CREATE TABLE IF NOT EXISTS `analytics.dim_products` (
  product_sk STRING NOT NULL,
  product_id STRING,
  product_name STRING,
  category STRING,
  price FLOAT64,
  cost FLOAT64,
  run_id STRING,
  load_time TIMESTAMP,
  is_current BOOL,
  PRIMARY KEY (product_sk) NOT ENFORCED
);

MERGE analytics.dim_products d
USING (
    SELECT DISTINCT p.product_id
    FROM raw1.products p
    JOIN analytics.dim_products d2
      ON p.product_id = d2.product_id
     AND d2.is_current = TRUE
    WHERE p.run_id = '{{ run_id }}'
      AND (
            d2.product_name != TRIM(p.product_name)
         OR d2.category != p.category
         OR d2.price    != p.price
         OR d2.cost     != p.cost
      )
) AS changed
ON d.product_id = changed.product_id
AND d.is_current = TRUE
WHEN MATCHED THEN
    UPDATE SET is_current = FALSE;


INSERT INTO `analytics.dim_products` (product_sk, product_id, product_name, category, price, cost, run_id, load_time, is_current)
SELECT
    GENERATE_UUID() AS product_sk,
    p.product_id,
    TRIM(p.product_name) AS product_name,
    p.category,
    p.price,
    p.cost,
    p.run_id,
    CURRENT_TIMESTAMP(),
    TRUE 
FROM raw1.products p
LEFT JOIN analytics.dim_products d
  ON p.product_id = d.product_id
 AND d.is_current = TRUE
WHERE p.run_id = '{{ run_id }}'
  AND (
        d.product_id IS NULL -- new record
     OR d.product_name != TRIM(p.product_name)
     OR d.category != p.category
     OR d.price != p.price
     OR d.cost != p.cost     
  );
  


