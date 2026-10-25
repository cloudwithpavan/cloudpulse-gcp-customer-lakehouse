# CloudPulse Architecture Notes

## Primary scheduled sensor pipeline

```mermaid
flowchart TD
    A[CSV uploaded to Cloud Storage] --> B[wait_for_csv<br/>GCSObjectExistenceSensor]
    B --> C[load_bronze<br/>GCS to BigQuery]
    C --> D[build_silver<br/>clean and standardize]
    D --> E[build_gold<br/>city-level aggregation]
    E --> F[validate_reconciliation<br/>row-count checks]
```

The sensor waits for the configured object to exist before downstream tasks proceed. Sensor polling interval and DAG scheduling interval are separate settings.

## Event-driven pipeline

```mermaid
flowchart TD
    A[Cloud Storage OBJECT_FINALIZE] --> B[Pub/Sub topic]
    B --> C[Subscription consumer / Cloud Run]
    C --> D[Trigger Airflow DAG with object_name]
    D --> E[Resolve and validate object path]
    E --> F[Bronze]
    F --> G[Silver]
    G --> H[Gold]
    H --> I[Reconciliation]
```

The event-driven pipeline requires the subscription consumer and Airflow trigger permissions to be correctly deployed. A topic and notification alone do not trigger an Airflow DAG.

## Screenshot index

See [`screenshots/`](screenshots/) for ordered evidence. The README embeds the main screenshots.
