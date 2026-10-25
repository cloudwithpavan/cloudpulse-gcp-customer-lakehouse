from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "my-project-77-511616")
GCS_BUCKET = os.environ.get("GCS_BUCKET_NAME", "")
GCS_OBJECT = os.environ.get("GCS_OBJECT_NAME", "incoming/customers.csv")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
BRONZE_DATASET = os.environ.get("BQ_BRONZE_DATASET", "customer_bronze_dev")
SILVER_DATASET = os.environ.get("BQ_SILVER_DATASET", "customer_silver_dev")
GOLD_DATASET = os.environ.get("BQ_GOLD_DATASET", "customer_gold_dev")

if not GCS_BUCKET:
    # Composer environment variable must be set by Terraform. A clear failure is
    # preferable to silently reading from an unintended bucket.
    GCS_BUCKET = "CONFIGURE_GCS_BUCKET_NAME"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="customer_lakehouse_sensor_pipeline",
    description="GCS sensor -> BigQuery Bronze -> Silver -> Gold customer analytics",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["cloudpulse", "gcs-sensor", "bronze-silver-gold"],
) as dag:

    wait_for_csv = GCSObjectExistenceSensor(
        task_id="wait_for_csv",
        bucket=GCS_BUCKET,
        object=GCS_OBJECT,
        google_cloud_conn_id="google_cloud_default",
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
        soft_fail=False,
    )

    load_bronze = GCSToBigQueryOperator(
        task_id="load_bronze",
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJECT],
        destination_project_dataset_table=f"{PROJECT_ID}.{BRONZE_DATASET}.customers_raw",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
    )

    build_silver = BigQueryInsertJobOperator(
        task_id="build_silver",
        project_id=PROJECT_ID,
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{SILVER_DATASET}.customers_cleaned` AS
                WITH typed AS (
                  SELECT
                    SAFE_CAST(customer_id AS INT64) AS customer_id,
                    TRIM(full_name) AS full_name,
                    LOWER(TRIM(email)) AS email,
                    INITCAP(TRIM(city)) AS city,
                    SAFE_CAST(salary AS NUMERIC) AS salary
                  FROM `{PROJECT_ID}.{BRONZE_DATASET}.customers_raw`
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
                  SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY customer_id ORDER BY email
                  ) AS row_num
                  FROM valid
                )
                WHERE row_num = 1
                """,
                "useLegacySql": False,
            }
        },
    )

    build_gold = BigQueryInsertJobOperator(
        task_id="build_gold",
        project_id=PROJECT_ID,
        location=BQ_LOCATION,
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{GOLD_DATASET}.customer_city_report`
                CLUSTER BY city AS
                SELECT
                  city,
                  COUNT(*) AS customer_count,
                  AVG(salary) AS average_salary,
                  SUM(salary) AS total_salary,
                  CURRENT_TIMESTAMP() AS refreshed_at
                FROM `{PROJECT_ID}.{SILVER_DATASET}.customers_cleaned`
                GROUP BY city
                """,
                "useLegacySql": False,
            }
        },
    )

    def validate_reconciliation(**context):
        from google.cloud import bigquery

        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
          SELECT
            (SELECT COUNT(*) FROM `{PROJECT_ID}.{BRONZE_DATASET}.customers_raw`) AS bronze_count,
            (SELECT COUNT(*) FROM `{PROJECT_ID}.{SILVER_DATASET}.customers_cleaned`) AS silver_count,
            (SELECT COUNT(*) FROM `{PROJECT_ID}.{GOLD_DATASET}.customer_city_report`) AS gold_count
        """
        row = next(iter(client.query(query, location=BQ_LOCATION).result()))
        print(
            f"Reconciliation: bronze={row.bronze_count}, "
            f"silver={row.silver_count}, gold_city_rows={row.gold_count}"
        )
        if row.bronze_count < row.silver_count:
            raise ValueError("Silver row count cannot exceed Bronze row count.")

    reconciliation = PythonOperator(
        task_id="validate_reconciliation",
        python_callable=validate_reconciliation,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    wait_for_csv >> load_bronze >> build_silver >> build_gold >> reconciliation
