variable "credentials"  {
  description = "Credentials for GCP"
  default     = "/home/lulu/projects/data_talks/nyc-collisions-experiments/terraform-mage-gcp/nyc-auto-accidents-terraform.json"
}  

variable "gcp_storage_location" {
  description = "Location for GCP bucket"
  default     = "EU"
}

variable project {
  description = "Project"
  default     = "nyc-auto-accidents"
}

variable "region" {
  description = "Region"
  default     = "europe-west4"
  #default    = "us-west4"
}

variable "zone" {
  type        = string
  description = "The default compute zone"
  default     = "europe-west4-b"
  #default     = "us-west4-a"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset"
  default     = "nyc_collisions_dataset"
}

variable "gcp_storage_class" {
  description = "Storage class for GCP bucket"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "Name for GCP bucket"
  default     = "nyc_collisions_bucket_1234"
}

variable "docker_image" {
  type        = string
  description = "The docker image to deploy to Cloud Run."
  default     = "mageai/mageai:latest"
}

variable "vm_instance_name" {
  description = "Name for GCP VM instance"
  default     = "nyc-collisions-vm"
}
