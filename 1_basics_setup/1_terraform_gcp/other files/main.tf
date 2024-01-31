terraform {
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)  # Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
  project     = var.project
  region      = var.region
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 30 // days
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}



resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}