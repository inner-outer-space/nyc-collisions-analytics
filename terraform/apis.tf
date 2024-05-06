# #############################################
# #               Enable API's                #
# #############################################
# Resource: https://stackoverflow.com/questions/59055395/can-i-automatically-enable-apis-when-using-gcp-cloud-with-terraform
#
# Enable the following 2 APIs via the console: 
#     gcloud services enable serviceusage.googleapis.com bigquery.googleapis.com

# Use `gcloud` to enable:
# - serviceusage.googleapis.com
# - bigquery.googleapis.com
resource "null_resource" "activate_service_account" {
  provisioner "local-exec" {
     command = "gcloud auth activate-service-account --key-file=${var.credentials}"
  }
}
resource "null_resource" "enable_service_usage_and_bq_api" {
  provisioner "local-exec" {
    command = "gcloud services enable serviceusage.googleapis.com bigquery.googleapis.com --project ${var.project}"
  }
  depends_on = [null_resource.activate_service_account]
}

# Wait for the new configuration to propagate
resource "time_sleep" "wait_api_init" {
  create_duration = "60s"
  depends_on = [null_resource.enable_service_usage_and_bq_api]
}

# Enable IAM API
resource "google_project_service" "iam" {
  service = "iam.googleapis.com"
  disable_on_destroy = false
  depends_on = [time_sleep.wait_api_init]
}

# Enable Cloud Resource Manager API
resource "google_project_service" "resourcemanager" {
  service = "cloudresourcemanager.googleapis.com"
  disable_on_destroy = false
  depends_on = [time_sleep.wait_api_init]
}
