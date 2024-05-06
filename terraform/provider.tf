# Terraform provider configuration for Google Cloud Platform

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.11.0"
    }
  }
}

provider "google" {
# Credentials set so you can run terraform apply without setting GOOGLE_APPLICATION_CREDENTIALS
    credentials = "../mage/google_cloud_key.json"
    project = var.project
    region  = var.region
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
  credentials = file(var.credentials)
}