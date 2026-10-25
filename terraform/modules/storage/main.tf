resource "random_id" "suffix" {
  byte_length = 3
}
resource "google_storage_bucket" "raw" {
  project                     = var.project_id
  name                        = "${var.name_prefix}-${random_id.suffix.hex}"
  location                    = var.location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy
  versioning { enabled = true }
  lifecycle { prevent_destroy = false }
}
output "bucket_name" { value = google_storage_bucket.raw.name }
