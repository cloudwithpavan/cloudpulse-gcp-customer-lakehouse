resource "google_bigquery_dataset" "bronze" {
  project                    = var.project_id
  dataset_id                 = "customer_bronze_${var.environment}"
  location                   = var.location
  delete_contents_on_destroy = false
  description                = "Bronze raw customer data for ${var.environment}"
}
resource "google_bigquery_dataset" "silver" {
  project                    = var.project_id
  dataset_id                 = "customer_silver_${var.environment}"
  location                   = var.location
  delete_contents_on_destroy = false
  description                = "Silver cleaned customer data for ${var.environment}"
}
resource "google_bigquery_dataset" "gold" {
  project                    = var.project_id
  dataset_id                 = "customer_gold_${var.environment}"
  location                   = var.location
  delete_contents_on_destroy = false
  description                = "Gold analytics outputs for ${var.environment}"
}
output "bronze_dataset_id" { value = google_bigquery_dataset.bronze.dataset_id }
output "silver_dataset_id" { value = google_bigquery_dataset.silver.dataset_id }
output "gold_dataset_id" { value = google_bigquery_dataset.gold.dataset_id }
