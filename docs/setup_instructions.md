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

    Confirm it has been correctly set </br>
   `echo $GOOGLE_APPLICATION_CREDENTIALS`

4. Authenticate on GCloud </br>
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

2. Run the following terraform commands to create GCP resources - **BigQuery DataSet, GCS Bucket, more APIs** </br>

    - `terraform init`
    - `terraform plan`
    - `terraform apply`


## MAGE 
1. In the Mage folder: <br/>
   - Change name of the `my.env` file to `.env`
   <br/>
2. In the `.env` file: </br>
   - GOOGLE_PROJECT_ID  --> _change to your google project id_
   <br/>
3. In the `mage/nyc-collisions/dbt/collisions/profiles.yml` file: </br>
   - dev > project -->  _change to your google project id_ 
   - prod > project -->  _change to your google project id_
   <br/>
4. In `mage/nyc-collisions/dbt/collisions/models/staging/schema.yml` update >/br>
   - database --> to your project_id
    <br/>
5. Run `docker-compose up`


# RUNNING THE PIPELINE 
<div align = center> !!! PLEASE CLEAR YOUR BROWSER CACHE BEFORE RUNNING THE PIPELINES !!! </div>


### EXTRACT THE HISTORIC COLLISIONS DATA (~30 min)
In the Scripts folder, run  </br> 
`./get_historic_api.sh 2015 2023` </br>
</br>
This script submits an api request to the mage 'monthly_extract_trigger' for each month between the start and end month specified (here jan 2015 - dec 2023). The pipeline sets global variables, makes batched requests to the NYC Open Data rest api until the full month of data is retrieved, and then writes the output raw parquet to the GCS bucket created previously with Terraform. </br>
</br>
The script includes a 20s pause between each pipeline run to avoid overwhelming the source. This step takes about ~30 min. 

### EXTRACT AND PROCESS THE WEATHER DATA (2 min)
Once the previous step is complete, execute the following command in the terminal: </br>
`curl -X POST FILL IN HERE \
  --header 'Content-Type: application/json'` </br>
</br>

### PROCESS THE COLLISION DATA 
Once the previous step is complete execute the following command in the terminal:</br>
''
</br>

  



## TAKING THE PROJECT DOWN 
When you are done with the project execute `terraform destroy` to take down the resources. Once the files and resources are removed, you can delete the project.   

note: a second dataset is created in BQ by DBT. This will need to be manually removed. 
