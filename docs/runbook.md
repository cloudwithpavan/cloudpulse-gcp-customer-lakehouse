# Operational runbook

## Expected run order
`wait_for_csv → load_bronze → build_silver → build_gold → validate_reconciliation`

## Sensor timeout
The sensor waits for up to one hour. Check the exact object name `incoming/customers.csv`, bucket configuration and Composer worker permissions. Upload the file while the DAG run is active, then inspect task logs.

## BigQuery failures
Check dataset location, service account permissions (`bigquery.jobUser` and dataset write access), table names, SQL job logs and Composer logs.

## Cost control
- Deploy only dev initially.
- Set a Cloud Billing budget alert.
- Destroy Composer after testing.
- Keep state storage separate from environment resources.
- Check for orphaned resources in GCP after destroy.
- Budget alerts notify; they do not cap or automatically stop spend.

## Recovery
Use the same Terraform environment folder and backend configuration used for deployment. Inspect `terraform state list` and `terraform plan` before destructive changes. If state is missing, restore from a backup/version or import existing resources before destroy.
