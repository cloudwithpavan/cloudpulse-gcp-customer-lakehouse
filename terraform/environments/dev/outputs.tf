output "raw_bucket_name" { value = module.storage.bucket_name }
output "bronze_dataset" { value = module.bigquery.bronze_dataset_id }
output "silver_dataset" { value = module.bigquery.silver_dataset_id }
output "gold_dataset" { value = module.bigquery.gold_dataset_id }
output "composer_airflow_uri" {
  value = var.deploy_composer ? module.composer[0].airflow_uri : null
}
