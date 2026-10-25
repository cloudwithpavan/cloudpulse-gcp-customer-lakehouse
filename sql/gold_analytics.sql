CREATE OR REPLACE TABLE `my-project-77-511616.customer_gold_dev.customer_city_report`
CLUSTER BY city AS
SELECT
  city,
  COUNT(*) AS customer_count,
  AVG(salary) AS average_salary,
  SUM(salary) AS total_salary,
  CURRENT_TIMESTAMP() AS refreshed_at
FROM `my-project-77-511616.customer_silver_dev.customers_cleaned`
GROUP BY city;
