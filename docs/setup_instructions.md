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
3. set the GOOGLE_APPLICATION_CREDENTIALS</br>
   `export GOOGLE_APPLICATION_CREDENTIALS=/your/path/to/this/google_cloud_key.json`

    double check with </br>
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
1. Change name of the `my.env` file to `.env`
2. In the .env file, update the following variables:
   - GOOGLE_PROJECT_ID
3. In mage/nyc-collisions/dbt/collisions/profiles.yml update: </br>
   - dev > project --> to your project_id
   - prod > project --> to your project_id
5. In mage/nyc-collisions/dbt/collisions/models/staging/schema.yml update >/br>
   - database --> to your project_id

## RUNNING THE PIPELINE 
1. In the Mage Folder run `docker-compose up`
2. Trigger the Extraction Pipeline via cURL. ** This will take at least 20 min** </br>
   `curl -X POST http://127.0.0.1:6789/api/pipeline_schedules/14/pipeline_runs/6f1fbec7c85b48b794a202618dcaef13`
4. Trigger the Data Processing Pipeline via the cURL to the API endpoint </br>
   `curl -X POST http://127.0.0.1:6789/api/pipeline_schedules/16/pipeline_runs/b28ad8aa9ba740ddb838dd9d36232a4f`
