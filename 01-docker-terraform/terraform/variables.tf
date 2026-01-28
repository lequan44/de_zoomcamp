variable "project" {
  default     = "de-zoomcamp-terraform-demo"
  description = "Project ID"
}

variable "region" {
  default     = "asia-southeast1"
  description = "region"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket Storage class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "My Storage bucket name"
  default     = "de-zoomcamp-terra-bucket"
}

variable "location" {
  default     = "ASIA"
  description = "Project location"
}

variable "credentials" {
  default     = "./keys/credentials.json"
  description = "My Credentials"
}