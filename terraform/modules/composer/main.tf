
resource "google_composer_environment" "this" {
  project = var.project_id
  name    = var.environment_name
  region  = var.region

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    software_config {
      image_version = var.image_version
      env_variables = var.environment_variables
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }

      triggerer {
        cpu       = 0.5
        memory_gb = 1
        count     = 1
      }



      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }

      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 2
      }
    }

    node_config {
      service_account = var.service_account_email
    }
  }
}

output "airflow_uri" {
  value = google_composer_environment.this.config[0].airflow_uri
}

output "dag_gcs_prefix" {
  value = google_composer_environment.this.config[0].dag_gcs_prefix
}
