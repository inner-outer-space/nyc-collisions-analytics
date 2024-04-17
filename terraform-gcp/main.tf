# main.tf

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.11.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
  credentials = file(var.credentials)
}

# #############################################
# #               Enable API's                #
# #############################################

# Enable IAM API
resource "google_project_service" "iam" {
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}

# Enable Cloud Resource Manager API
resource "google_project_service" "resourcemanager" {
  service            = "cloudresourcemanager.googleapis.com"
  disable_on_destroy = false
}

# Enable Cloud SQL Admin API
resource "google_project_service" "sqladmin" {
  service            = "sqladmin.googleapis.com"
  disable_on_destroy = false
}

# #############################################
# #               Enable Services             #
# #############################################

resource "google_storage_bucket" "collisions-bucket" {
  name          = var.gcs_bucket_name
  location      = var.gcp_storage_location
  force_destroy = true
}

resource "google_bigquery_dataset" "collisions-dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.gcp_storage_location
}

resource "google_compute_instance" "vm_instance" {
  name         = var.vm_instance_name
  machine_type = "f1-micro"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }
  
}