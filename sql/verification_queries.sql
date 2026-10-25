-- Bronze: raw records
SELECT * FROM `my-project-77-511616.customer_bronze_dev.customers_raw` LIMIT 100;

-- Silver: cleaned, typed, deduplicated records
SELECT * FROM `my-project-77-511616.customer_silver_dev.customers_cleaned`
ORDER BY customer_id;

-- Gold: business-facing metrics
SELECT * FROM `my-project-77-511616.customer_gold_dev.customer_city_report`
ORDER BY city;

-- Layer reconciliation
SELECT
  (SELECT COUNT(*) FROM `my-project-77-511616.customer_bronze_dev.customers_raw`) AS bronze_rows,
  (SELECT COUNT(*) FROM `my-project-77-511616.customer_silver_dev.customers_cleaned`) AS silver_rows,
  (SELECT COUNT(*) FROM `my-project-77-511616.customer_gold_dev.customer_city_report`) AS gold_city_rows;
