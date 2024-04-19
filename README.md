# nyc-collisions-analytics

Work in progress


meteo data 
https://www.worldweatheronline.com/weather-api/api/docs/historical-weather-api.aspx



# SET UP 

## clone this repo 
1. `git clone git@github.com:inner-outer-space/nyc-collisions-analytics.git`
2. change the my.env to .env 

## Google Cloud Platform  
1. Create a project in GCP
2. Add a service account
    - For simplicity, set role to owner 
    - Add storage admin role 
    - Download the key to the the mage folder in this repo as `google_cloud_key.json`
3. set the GOOGLE_APPLICATION_CREDENTIALS
   ``` bash
   export GOOGLE_APPLICATION_CREDENTIALS = '../mage/google_cloud_key.json'
   # double check with 
   echo $GOOGLE_APPLICATION_CREDENTIALS
   ```
4. Authenticate on GCloud
   `gcloud auth application-default login`

    
## MAGE 
1. Move the service account json key to the the mage folder and name it `google_cloud_key.json`
2. Change the my.env to .env 

## TERRAFORM  
1. In the terraform directory update the defaults for following variables in the variables.tf file:
    -  gcp storage location 
    -  project name
    -  region
    -  zone

Terraform Info: https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform/1_terraform_gcp/terraform
cd into the terraform dir
4. `terraform init`
5. `terraform plan`
6. `terraform apply`
