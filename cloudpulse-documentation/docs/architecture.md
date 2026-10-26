# CloudPulse Architecture

```mermaid
flowchart TB
  CSV[Customer CSV] --> GCS[Cloud Storage / incoming]
  GCS --> SENSOR[GCSObjectExistenceSensor]
  SENSOR --> BRONZE[BigQuery Bronze]
  BRONZE --> SILVER[BigQuery Silver]
  SILVER --> GOLD[BigQuery Gold]
  GOLD --> CHECK[Reconciliation]
  GCS -. OBJECT_FINALIZE .-> PS[Pub/Sub]
  PS --> CR[Cloud Run handler]
  CR --> EVENT[Unscheduled Airflow event DAG]
  EVENT --> BRONZE
```

The sensor DAG is scheduled separately from the event-driven DAG. The event path requires a working notification, Pub/Sub delivery, Cloud Run handler, IAM configuration, and Airflow DAG-trigger call.
