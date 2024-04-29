# STEPS TO REPRODUCE

### Prerequisites
- GCP access
- Google Cloud CLI
- Docker
- Terraform 

## Clone the Repo </br>
`git clone git@github.com:inner-outer-space/nyc-collisions-analytics.git`

 
## Google Cloud Platform  
1. Create a new project on GCP
2. Add a service account
    - Go to IAM & Admin > Service Account and `+ CREATE SERVICE ACCOUNT `.
    - For simplicity, set the Role to owner and leave the other fields blank. 
    - Under Actions for the service account, click  `Manage Keys`.
    - Create a new JSON key and download it to the the mage folder in this repo as `google_cloud_key.json`.
3. Set the GOOGLE_APPLICATION_CREDENTIALS</br>
   `export GOOGLE_APPLICATION_CREDENTIALS=/your/path/to/this/google_cloud_key.json`

    Double check they have been correctly set </br>
   `echo $GOOGLE_APPLICATION_CREDENTIALS`

4. Authenticate on GCloud
   `gcloud auth application-default login`
   
6. Enable the following APIs:
   - Service Usage API
   - Big Query API
  
7. If you are using a paid account rather than the free trial, link a Billing Account to the Project.
8. Wait a few minutes for the previous steps to take affect. 
   
## TERRAFORM
In the Terraform folder: <br/>
1. In the variables.tf file, update the default values for the following variables:
    -  project name  (required)
    -  gcp storage location (if needed)
    -  region (if needed)
    -  zone (if needed)

2. Run the following terraform commands to create GCP resources - BigQuery DataSet, GCS Bucket, more APIs </br>
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
In the Mage Folder: 
1. Run `docker-compose up`
2. Trigger the Extraction Pipeline via cURL. ** This will take at least 20 min** </br>
   `curl -X POST http://127.0.0.1:6789/api/pipeline_schedules/14/pipeline_runs/6f1fbec7c85b48b794a202618dcaef13`
3. Trigger the Data Processing Pipeline via the cURL </br>
   `curl -X POST http://127.0.0.1:6789/api/pipeline_schedules/16/pipeline_runs/b28ad8aa9ba740ddb838dd9d36232a4f`


## TAKING THE PROJECT DOWN 
When you are done with the project execute `terraform destroy` to take down the resources. Once the files and resources are removed, you can delete the project.   

note: a second dataset is created in BQ by DBT. This will need to be manually removed. 
