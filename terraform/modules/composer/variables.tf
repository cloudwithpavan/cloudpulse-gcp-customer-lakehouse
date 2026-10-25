variable "project_id" { type = string }
variable "region" { type = string }
variable "environment_name" { type = string }
variable "service_account_email" { type = string }
variable "image_version" {
  description = "Optional Cloud Composer image version."
  type        = string
  default     = null
  nullable    = true
}
variable "environment_variables" { type = map(string) }
