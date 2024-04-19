# nyc-collisions-analytics

Work in progress

## Description

### Objective

### Dataset
meteo data 
https://www.worldweatheronline.com/weather-api/api/docs/historical-weather-api.aspx

### Tools and Technologies

- Cloud - [**Google Cloud Platform**](https://cloud.google.com)
- Infrastructure as Code software - [**Terraform**](https://www.terraform.io)
- Containerization - [**Docker**](https://www.docker.com), [**Docker Compose**](https://docs.docker.com/compose/)
- Orchestration - [**Mage**](https://www.mage.ai/)
- Transformation and Batch Processing - [**Apache Spark**](https://spark.apache.org/)
- Data Lake - [**Google Cloud Storage**](https://cloud.google.com/storage)
- Data Warehouse - [**BigQuery**](https://cloud.google.com/bigquery)
- Data Visualization - [**Looker Studio**](https://support.google.com/looker-studio/community?hl=en&sjid=8237525324739923528-EU)
- Language - [**Python**](https://www.python.org)




# STEPS TO REPRODUCE

## clone this repo 
1. `git clone git@github.com:inner-outer-space/nyc-collisions-analytics.git`
2. change the my.env to .env 

## Google Cloud Platform  
1. Create a new project on GCP
2. Add a service account
    - Go to IAM & Admin > Service Account and `+ CREATE SERVICE ACCOUNT `
    - For simplicity, set the Role to owner. Leave other fields blank. 
    - Under Actions for the service account, click  `Manage Keys`
    - Create a new JSON key and download it to the the mage folder in this repo as `google_cloud_key.json`
3. set the GOOGLE_APPLICATION_CREDENTIALS
   `export GOOGLE_APPLICATION_CREDENTIALS=/your/path/to/this/google_cloud_key.json`

    double check with 
   `echo $GOOGLE_APPLICATION_CREDENTIALS`

4. Authenticate on GCloud
   `gcloud auth application-default login`
5. Enable the following APIs
   - Service Usage API
   - Big Query API
   
## TERRAFORM
In the Terraform folder:
1. In the variables.tf file, update the default values for the following variables:
    -  project name  (required)
    -  gcp storage location (if needed)
    -  region (if needed)
    -  zone (if needed)

2. Create or activate GCP resources - BigQuery DataSet, GCS Bucket, APIs </br>
[**More on Terraform Commands**]([https://cloud.google.com](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform))
    - `terraform init`
    - `terraform plan`
    - `terraform apply`

## MAGE 
In the Mage folder: 
1. Change the my.env to .env
2. In the .env file, update the following variables:
   - GOOGLE_PROJECT_ID
3. In mage/nyc-collisions/dbt/collisions/profiles.yml update the project and dataset 
4. In mage/nyc-collisions/dbt/collisions/models/staging/schema.yml update database (project_id) and schema (bq dataset)
