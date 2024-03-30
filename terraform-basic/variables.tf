variable "credentials"  {
  description = "Credentials for GCP"
  default     = "~/.gcp-keys/nyc-collisions-terraform-key.json"
}  

variable "gcp_storage_location" {
  description = "Location for GCP bucket"
  default     = "EU"
}

variable project {
  description = "Project"
  default     = "nyc-collisions-418717"
}

variable "region" {
  description = "Region"
  default     = "europe-west1"
  
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


