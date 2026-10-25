# Architecture and design decisions

## Data layers
- **Bronze:** raw landed CSV loaded into `customers_raw`. This is a replayable ingestion layer.
- **Silver:** typed, trimmed, lowercased and deduplicated customer records; invalid required fields and invalid salaries are excluded.
- **Gold:** aggregated city metrics for reporting.

## Orchestration
`GCSObjectExistenceSensor` runs in reschedule mode, polling at 60-second intervals for up to 3600 seconds. A successful sensor is a prerequisite for all downstream work. The sensor does not start a DAG run by itself.

## Idempotency
- Bronze uses `WRITE_TRUNCATE` for the fixed sample object.
- Silver and Gold use `CREATE OR REPLACE TABLE`.
- `max_active_runs=1` avoids concurrent DAG runs writing to the same tables.
- For production incremental ingestion, introduce immutable file manifests, batch IDs, audit tables and a MERGE strategy instead of replacing the full table.

## Environments
Dev, preprod and prod are separate deployments with unique resource names and remote-state prefixes. For strong production isolation, use separate GCP projects and separate identities.
