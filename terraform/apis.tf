# #############################################
# #               Enable API's                #
# #############################################
# Enable the following 2 APIs via the console: 
#   - gcloud services enable serviceusage.googleapis.com
#   - gcloud services enable bigquery.googleapis.com


# Enable IAM API
resource "google_project_service" "iam" {
  service = "iam.googleapis.com"
  disable_on_destroy = false
}

# Enable Cloud Resource Manager API
resource "google_project_service" "resourcemanager" {
  service = "cloudresourcemanager.googleapis.com"
  disable_on_destroy = false
}

# This needs to be enabled manually in the GCP console
# resource "google_project_service" "serviceusage" {
#   project = var.project
#   service = "serviceusage.googleapis.com"
#   disable_on_destroy = true
# }

# # This needs to be enabled manually in the GCP console
# resource "google_project_service" "bigquery" {
#   service = "bigquery.googleapis.com"
#   disable_on_destroy = true
# }
