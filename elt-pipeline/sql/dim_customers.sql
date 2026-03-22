CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_sk STRING NOT NULL,
    customer_id STRING,
    customer_name STRING,
    city STRING,
    run_id STRING,
    load_time TIMESTAMP,
    is_current BOOL,
    PRIMARY KEY (customer_sk) NOT ENFORCED
);

MERGE analytics.dim_customers d
USING (
    SELECT DISTINCT c.customer_id
    FROM raw1.customer c
    JOIN analytics.dim_customers d2
      ON c.customer_id = d2.customer_id
     AND d2.is_current = TRUE
    WHERE c.run_id = '{{ run_id }}'
      AND (
            d2.customer_name != TRIM(c.customer_name)
         OR d2.city != c.city
      )
) AS changed
ON d.customer_id = changed.customer_id
AND d.is_current = TRUE

WHEN MATCHED THEN
    UPDATE SET is_current = FALSE;

-- only insert new records or changed records

INSERT INTO analytics.dim_customers (
    customer_sk,
    customer_id,
    customer_name,
    city,
    run_id,
    load_time,
    is_current
)
SELECT
    GENERATE_UUID() AS customer_sk,
    c.customer_id,
    TRIM(c.customer_name),
    c.city,
    c.run_id,
    CURRENT_TIMESTAMP(),
    TRUE
FROM raw1.customer c
LEFT JOIN analytics.dim_customers d
  ON c.customer_id = d.customer_id
 AND d.is_current = TRUE
WHERE c.run_id = '{{ run_id }}'
  AND (
        d.customer_id IS NULL -- new record
     OR d.customer_name != TRIM(c.customer_name)
     OR d.city != c.city      
  );
