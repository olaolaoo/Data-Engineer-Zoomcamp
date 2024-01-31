locals {
  data_lake_bucket = "dtc_data_lake"
  location         = "US"
  region           = "us-central1"
}

variable "project" {
  description = "Your GCP Project ID"
  default     = "coherent-ascent-379901"
}


variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type        = string
  default     = "trips_data_all"
}
