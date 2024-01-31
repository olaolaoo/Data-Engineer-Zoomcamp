terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.13.0"
    }
  }
}

provider "google" {
  //credentials = "./keys/my-creds.json" # Credentials only needs to be set if you do not have the GOOGLE_APPLICATION_CREDENTIALS set
  project = var.project
  region  = local.region
}



resource "google_storage_bucket" "demo-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}"
  location      = local.location
  force_destroy = true
  # Optional, but recommended settings:
  storage_class = var.storage_class

  lifecycle_rule {
    condition {
      age = 2 //days
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}


resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name

}

