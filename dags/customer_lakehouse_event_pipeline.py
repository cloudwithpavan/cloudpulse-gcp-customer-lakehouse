
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET = os.environ["GCS_BUCKET_NAME"]
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
BRONZE_DATASET = os.environ.get("BQ_BRONZE_DATASET", "customer_bronze_dev")
SILVER_DATASET = os.environ.get("BQ_SILVER_DATASET", "customer_silver_dev")
GOLD_DATASET = os.environ.get("BQ_GOLD_DATASET", "customer_gold_dev")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def get_uploaded_object(**context):
    """Read the GCS object path passed by the event trigger."""
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    object_name = conf.get("object_name")

    if not object_name:
        raise ValueError(
            "Missing object_name in DAG run configuration. "
            "Pass the uploaded GCS object path."
        )

    if not object_name.lower().endswith(".csv"):
        raise ValueError("Only CSV files are supported.")

    if not object_name.startswith("incoming/"):
        raise ValueError("CSV files must be inside the incoming/ folder.")

    if ".." in object_name.split("/"):
        raise ValueError("Invalid object path.")

    print(f"Processing uploaded object: gs://{GCS_BUCKET}/{object_name}")
    return object_name


with DAG(
    dag_id="customer_lakehouse_event_pipeline",
    description="Event-triggered CSV ingestion into Bronze, Silver and Gold",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["cloudpulse", "event-driven", "bronze-silver-gold"],
) as dag:

    resolve_uploaded_file = PythonOperator(
        task_id="resolve_uploaded_file",
        python_callable=get_uploaded_object,
    )

    load_bronze = GCSToBigQueryOperator(
        task_id="load_bronze",
        bucket=GCS_BUCKET,
        source_objects=[
            "{{ ti.xcom_pull(task_ids='resolve_uploaded_file') }}"
        ],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.{BRONZE_DATASET}.customers_raw"
        ),
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
                CREATE OR REPLACE TABLE
                  `{PROJECT_ID}.{SILVER_DATASET}.customers_cleaned` AS
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
                  SELECT *,
                    ROW_NUMBER() OVER (
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
                CREATE OR REPLACE TABLE
                  `{PROJECT_ID}.{GOLD_DATASET}.customer_city_report`
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
          (SELECT COUNT(*) FROM
            `{PROJECT_ID}.{BRONZE_DATASET}.customers_raw`) AS bronze_count,
          (SELECT COUNT(*) FROM
            `{PROJECT_ID}.{SILVER_DATASET}.customers_cleaned`) AS silver_count,
          (SELECT COUNT(*) FROM
            `{PROJECT_ID}.{GOLD_DATASET}.customer_city_report`) AS gold_count
        """
        row = next(
            iter(client.query(query, location=BQ_LOCATION).result())
        )
        print(
            f"Reconciliation: bronze={row.bronze_count}, "
            f"silver={row.silver_count}, gold_city_rows={row.gold_count}"
        )
        if row.bronze_count < row.silver_count:
            raise ValueError(
                "Silver row count cannot exceed Bronze row count."
            )

    reconciliation = PythonOperator(
        task_id="validate_reconciliation",
        python_callable=validate_reconciliation,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    (
        resolve_uploaded_file
        >> load_bronze
        >> build_silver
        >> build_gold
        >> reconciliation
    )
