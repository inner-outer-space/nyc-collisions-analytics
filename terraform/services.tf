# #############################################
# #               Enable Services             #
# #############################################

resource "google_storage_bucket" "collisions-bucket" {
  name = var.gcs_bucket_name
  location = var.gcp_storage_location
  force_destroy = true
  depends_on = [time_sleep.wait_api_init]
}

resource "google_bigquery_dataset" "collisions-dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.gcp_storage_location
  depends_on = [time_sleep.wait_api_init]
}