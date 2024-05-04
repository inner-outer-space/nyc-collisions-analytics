variable "credentials"  {
  description = "Credentials for GCP"
  default     = "../mage/google_cloud_key.json"
}  
variable project {
  description = "Project"
  default     = "collisions-pipeline"     # This must be changed to your GCP project
}
variable "gcs_bucket_name" {
  description = "Name for GCP bucket"
  default     = "nyc-collisions-bucket"   # This must be changed to your globally unique bucket name
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset"
  default     = "nyc_collisions_dataset"  
}
variable "gcp_storage_location" {
  description = "Location for GCP bucket"
  default     = "EU"                      # change to your GCP location
}

variable "region" {
  description = "Region"
  default     = "europe-west1"            # change to your GCP region
  
}

variable "zone" {
  type        = string
  description = "The default compute zone"
  default     = "europe-west1-b"          # change to your GCP zone
}

variable "gcp_storage_class" {
  description = "Storage class for GCP bucket"
  default     = "STANDARD"
}

#variable "vm_instance_name" {
#  description = "Name for GCP VM instance"
#  default     = "nyc-collisions-vm"
#}
