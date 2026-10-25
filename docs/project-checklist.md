# CloudPulse checklist — dev only

- [ ] Confirm GCP project ID is `my-project-77-511616`.
- [ ] Confirm billing is enabled and permissions allow API enablement, IAM, GCS, BigQuery and Cloud Composer.
- [ ] Install Git, Python 3.11+, Google Cloud CLI, and Terraform 1.5+.
- [ ] Run local tests and `terraform fmt -check -recursive`.
- [ ] Create the Terraform state bucket with `terraform/standard Terraform workflow` and keep its state file safe.
- [ ] Initialise the dev backend with the state bucket.
- [ ] Review `terraform plan` and expected Composer cost before applying.
- [ ] Apply only `terraform/environments/dev`.
- [ ] Open Airflow, trigger `customer_lakehouse_sensor_pipeline`, and verify all tasks.
- [ ] Verify Bronze, Silver and Gold tables in BigQuery.
- [ ] Capture screenshots only after the real deployment succeeds.
- [ ] Push the final validated files to a new GitHub repository.
- [ ] Destroy the dev environment using the same Terraform backend when finished; retain state until cleanup is complete.
