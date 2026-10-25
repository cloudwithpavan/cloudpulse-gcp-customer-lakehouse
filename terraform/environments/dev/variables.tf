variable "project_id" {
  type    = string
  default = "my-project-77-511616"
}
variable "region" {
  type    = string
  default = "us-central1"
}
variable "location" {
  type    = string
  default = "US"
}
variable "environment" {
  type    = string
  default = "dev"
}
variable "deploy_composer" {
  type        = bool
  description = "Set false for cost-free infrastructure/code checks; set true only when ready to pay for Composer."
  default     = false
}
variable "gcs_file" {
  type    = string
  default = "incoming/customers.csv"
}
