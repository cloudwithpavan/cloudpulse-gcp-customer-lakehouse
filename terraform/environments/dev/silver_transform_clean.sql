CREATE OR REPLACE TABLE `my-project-77-511616.customer_silver_dev.customers_cleaned` AS
WITH typed AS (
  SELECT
    SAFE_CAST(customer_id AS INT64) AS customer_id,
    TRIM(full_name) AS full_name,
    LOWER(TRIM(email)) AS email,
    INITCAP(TRIM(city)) AS city,
    SAFE_CAST(salary AS NUMERIC) AS salary
  FROM `my-project-77-511616.customer_bronze_dev.customers_raw`
),
valid AS (
  SELECT *
  FROM typed
  WHERE customer_id IS NOT NULL
    AND full_name IS NOT NULL AND full_name != ''
    AND email IS NOT NULL AND email != ''
    AND city IS NOT NULL AND city != ''
    AND salary IS NOT NULL AND salary >= 0
)
SELECT * EXCEPT(row_num)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY email) AS row_num
  FROM valid
)
WHERE row_num = 1;
