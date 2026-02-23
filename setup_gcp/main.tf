terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.20"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


# resource "google_storage_bucket" "demo_bucket" {
#   name          = var.gcs_bucket_name
#   location      = var.location
#   force_destroy = true

#   lifecycle_rule {
#     condition {
#       age = 1
#     }
#     action {
#       type = "AbortIncompleteMultipartUpload"
#     }
#   }
# }

# resource "google_bigquery_dataset" "demo_dataset" {
#   dataset_id = var.bq_dataset_name
#   location   = var.location
# }

resource "google_storage_bucket" "nyc_taxi_bucket" {
  name          = "de_zoomcamp_03_nyc_taxi"
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
