resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "composer.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com"
  ])
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_service_account" "composer" {
  project      = var.project_id
  account_id   = "p2-customer-lakehouse-dev"
  display_name = "CloudPulse customer lakehouse dev Composer worker"
  depends_on   = [google_project_service.apis]
}

resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.composer.email}"
}

module "storage" {
  source        = "../../modules/storage"
  project_id    = var.project_id
  location      = var.location
  name_prefix   = "p2-customer-raw-dev"
  force_destroy = false
}

module "bigquery" {
  source      = "../../modules/bigquery"
  project_id  = var.project_id
  location    = var.location
  environment = var.environment
}

resource "google_bigquery_dataset_iam_member" "composer_bronze_editor" {
  project    = var.project_id
  dataset_id = module.bigquery.bronze_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_bigquery_dataset_iam_member" "composer_silver_editor" {
  project    = var.project_id
  dataset_id = module.bigquery.silver_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_bigquery_dataset_iam_member" "composer_gold_editor" {
  project    = var.project_id
  dataset_id = module.bigquery.gold_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_storage_bucket_iam_member" "composer_read_raw" {
  bucket = module.storage.bucket_name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_storage_bucket_iam_member" "composer_write_raw" {
  bucket = module.storage.bucket_name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.composer.email}"
}




resource "google_pubsub_topic" "gcs_upload_events" {
  project = var.project_id
  name    = "cloudpulse-gcs-upload-events"

  depends_on = [google_project_service.apis]
}

resource "google_pubsub_topic_iam_member" "gcs_publish" {
  project = var.project_id
  topic   = google_pubsub_topic.gcs_upload_events.name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
}

data "google_project" "project" {
  project_id = var.project_id
}

resource "google_storage_notification" "gcs_upload_notification" {
  bucket         = module.storage.bucket_name
  payload_format = "JSON_API_V1"
  topic          = google_pubsub_topic.gcs_upload_events.id
  event_types    = ["OBJECT_FINALIZE"]
  depends_on     = [google_pubsub_topic_iam_member.gcs_publish]
}


module "composer" {
  count                 = var.deploy_composer ? 1 : 0
  source                = "../../modules/composer"
  project_id            = var.project_id
  region                = var.region
  environment_name      = "p2-customer-lakehouse-dev"
  service_account_email = google_service_account.composer.email
  environment_variables = {
    GCP_PROJECT_ID    = var.project_id
    GCS_BUCKET_NAME   = module.storage.bucket_name
    GCS_OBJECT_NAME   = var.gcs_file
    BQ_LOCATION       = var.location
    BQ_BRONZE_DATASET = module.bigquery.bronze_dataset_id
    BQ_SILVER_DATASET = module.bigquery.silver_dataset_id
    BQ_GOLD_DATASET   = module.bigquery.gold_dataset_id
  }
  depends_on = [
    google_project_iam_member.composer_worker,
    google_project_iam_member.bigquery_job_user,
    google_bigquery_dataset_iam_member.composer_bronze_editor,
    google_bigquery_dataset_iam_member.composer_silver_editor,
    google_bigquery_dataset_iam_member.composer_gold_editor,
    google_storage_bucket_iam_member.composer_read_raw,
    google_storage_bucket_iam_member.composer_write_raw,
    module.bigquery
  ]
}

resource "google_storage_bucket_object" "dag" {
  count      = var.deploy_composer ? 1 : 0
  name       = "dags/customer_lakehouse_sensor_pipeline.py"
  bucket     = split("/", replace(module.composer[0].dag_gcs_prefix, "gs://", ""))[0]
  source     = "${path.module}/../../../dags/customer_lakehouse_sensor_pipeline.py"
  depends_on = [module.composer]
}
