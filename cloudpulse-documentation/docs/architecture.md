# CloudPulse — Architecture

## Scheduled sensor pipeline

```mermaid
flowchart TD
    A[CSV in Cloud Storage] --> B[wait_for_csv: GCSObjectExistenceSensor]
    B --> C[Load Bronze]
    C --> D[Transform Silver]
    D --> E[Aggregate Gold]
    E --> F[Validate reconciliation]
```

## Event-driven extension

```mermaid
flowchart TD
    A[Cloud Storage OBJECT_FINALIZE] --> B[Pub/Sub topic]
    B --> C[Subscription consumer / Cloud Run]
    C --> D[Trigger Airflow DAG with object_name]
    D --> E[Validate object path]
    E --> F[Bronze]
    F --> G[Silver]
    G --> H[Gold]
    H --> I[Reconciliation]
```

The sensor pipeline and event-driven pipeline are separate DAGs. The event-driven path requires the Pub/Sub consumer and permissions to be deployed and connected; a topic/notification by itself does not trigger Airflow.
