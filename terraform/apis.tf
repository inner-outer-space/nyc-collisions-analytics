# #############################################
# #               Enable API's                #
# #############################################

# Enable IAM API
resource "google_project_apis" "iam" {
  service = "iam.googleapis.com"
  disable_on_destroy = false
}

# Enable Cloud Resource Manager API
resource "google_project_apis" "resourcemanager" {
  service = "cloudresourcemanager.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_apis" "serviceusage" {
  project = var.project
  service = "serviceusage.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_apis" "bigquery" {
  service = "bigquery.googleapis.com"
  disable_on_destroy = false
}

# # Enable Cloud SQL Admin API
# resource "google_project_apis" "sqladmin" {
#   service = "sqladmin.googleapis.com"
#   disable_on_destroy = false
# }