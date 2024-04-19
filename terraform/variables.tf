variable "credentials"  {
  description = "Credentials for GCP"
  default     = "../mage/google_cloud_key.json"
}  

variable "gcp_storage_location" {
  description = "Location for GCP bucket"
  default     = "EU"
}

variable project {
  description = "Project"
  default     = "collisions-pipeline"
}

variable "region" {
  description = "Region"
  default     = "europe-west1"
  
}

variable "zone" {
  type        = string
  description = "The default compute zone"
  default     = "europe-west1-b"
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
  default     = "nyc-collisions-bucket"
}


#variable "vm_instance_name" {
#  description = "Name for GCP VM instance"
#  default     = "nyc-collisions-vm"
#}
