# STEPS TO REPRODUCE
_Instructions for Linux_ 
### Prerequisites
- Google Cloud Platform Account
- Google Cloud CLI
- Docker
- Terraform 

## Clone the Repo </br>
`git clone git@github.com:inner-outer-space/nyc-collisions-analytics.git`

 
## Google Cloud Platform  
1. Create a new project on GCP.
2. Add a service account.
    - Go to IAM & Admin > Service Account and click `+ CREATE SERVICE ACCOUNT `.
    - For simplicity, set the Role to Owner.
    - Leave the other fields blank.
    - Click `Done`
3. Download a Service Account JSON Key. 
    - On the Service Account page, in the Actions menu for the service account, click  `Manage Keys`.
    - Create a new JSON key and download it to the MAGE FOLDER in this repo as `google_cloud_key.json`.
4. Make sure that billing is activated for the project (if you are using a paid account). 
<br/>

## TERRAFORM
In the Terraform folder: <br/>
1. Update variable in the variables.tf file: 
 - REQUIRED: 
    -  **project name**   _To the name of the project you just created_
    -  **bucket name**  _Pick one that is globally unique_ <br/>
 - IF YOURS DIFFERS FROM MINE: 
    -  **gcp storage location** 
    -  **region** 
    -  **zone**

2. Run the following terraform commands to create GCP resources - **Enable APIs, BigQuery DataSet, GCS Bucket** </br>

    - `terraform init`
    - `terraform plan`
    - `terraform apply`


## MAGE 
1. In the Mage folder: <br/>
   - Change name of the `my.env` file to `.env`
   <br/>
2. In `.env` update: </br>
   - GOOGLE_PROJECT_ID  --> _change to your google project id_
   - GOOGLE_BUCKET --> _change to your google bucket name_
   <br/>
3. In `mage/nyc-collisions/dbt/collisions/profiles.yml` update: </br>
   - dev > project -->  _change to your google project id_ 
   - prod > project -->  _change to your google project id_
   <br/>
4. In `mage/nyc-collisions/dbt/collisions/models/staging/schema.yml` update: </br>
   - database -->  _change to your google project id_
    <br/>
5. Run `docker-compose up`


# RUNNING THE PIPELINES 

### EXTRACT, PROCESS, AND STAGE THE WEATHER DATA (2 min)
The weather data used in this project was retrieved during an introductory free trial period for World Weather Online. The data was extracted and stored in CSV format for use in this pipeline. This script retrieves the CSV file from its GIT location, does some light processing, uploads it to GCS, and then creates an associated external table that can be accessed in BigQuery.  <br/> <br/>

1. Goto `http://localhost:6789/pipelines/weather_extract_and_prep_full_data/triggers`
2. Click `Run@once` and then `Run now`

### EXTRACT THE HISTORIC COLLISIONS DATA (~36 min)
This pipeline runs a loop for all of the months within 2015 - 2023 and script tirggers the 'collisions_extract_monthly_from_api' pipeline for each month. The monthly pipeline makes batched requests to the NYC Open Data rest api until the full month of data is retrieved and then ouputs the full months data as a parquet to GCS. The script includes a 20 pause between each month to be respectful of the source.

1. Goto `http://localhost:6789/pipelines/collisions_extract_all/triggers`
2. Click `Run@once` and then `Run now`

### PROCESS COLLISION DATA AND INCORMPORATE WEATHER (90 min)
This triggers the collisions_process_all pipeline which reads in a list of the monthly files created in the extraction set. The collisions_process_batch pipeline is triggered within this pipeline for each file in the list. Local spark is used to create a datetime stamp, asign data types, and calculate the sun phase (day, dusk, dawn, dark) at the time of each collision. DBT is used to further transform the collision data and enrich it with the weather data. The final data is incrementally added to the BigQuery fact table. The pipeline then creates annual, monthly, and vehicle dimension tables.   

1. Goto `http://localhost:6789/pipelines/weather_extract_and_prep_full_data/triggers`
2. Click `Run@once` and then `Run now`

### VIEW THE DATA IN BIG QUERY 
![image](https://github.com/inner-outer-space/nyc-collisions-analytics/assets/12296455/b9dc0ba7-33a7-4151-9325-c5564a80fa5b)


## TAKING THE PROJECT DOWN 
When you are done with the project execute `terraform destroy` to take down the resources. Once the files and resources are removed, you can delete the project.  

### EXTRACT THE HISTORIC COLLISIONS DATA (~30 min)
`curl -X POST http://localhost:6789/api/pipeline_schedules/22/pipeline_runs/b56488f84b104f1aaf789347a109b1c0`

In the Scripts folder: </br> 
1. Grant yourself execution privilidges on all the files<br/>
   `chmod +x *`
2. Run the script that retrieves the historic collisions data for Jan 2015 - Dec 2023 <br/>
   `./get_historic_api.sh 2015 2023` </br>
3. Monitor the pipeline at `http://localhost:6789/pipelines/collisions_extract_monthly_from_api/triggers`  
</br>

 - This script tirggers the 'collisions_extract_monthly_from_api' pipeline	for each month within the start and end years specified.
 - The pipeline makes batched requests to the NYC Open Data rest api until the full month of data is retrieved and then writes the output parquet to the GCS bucket.
 - The script includes a pause between each pipeline run to avoid overwhelming the source. 
</br>

### EXTRACT, PROCESS, AND STAGE THE WEATHER DATA (2 min)
Once the previous step is complete, execute the following command in the terminal: </br>
`curl -X POST http://localhost:6789/api/pipeline_schedules/17/pipeline_runs/ea94aa87644445e08b0ff330ec66b17a` </br>
</br>
The weather data used in this project was retrieved during an introductory free trial period for World Weather Online. The data was extracted and stored in CSV format for use in this pipeline. This script retrieves the CSV file from its GIT location, does some light processing, uploads it to GCS, and then creates an associated external table that can be accessed in BigQuery.  

### PROCESS COLLISION DATA AND INCORMPORATE WEATHER (90 min)
Once the previous step is complete execute the following command in the terminal:</br>
`curl -X POST http://localhost:6789/api/pipeline_schedules/19/pipeline_runs/877f905db450443292b277fe6af18537` </br>
</br>
This triggers the collisions_process_all pipeline which reads in a list of the monthly files created in the extraction set. The collisions_process_batch pipeline is triggered within this pipeline for each file in the list. Local spark is used to create a datetime stamp, asign data types, and calculate the sun phase (day, dusk, dawn, dark) at the time of each collision. DBT is used to further transform the collision data and enrich it with the weather data. The final data is incrementally added to the BigQuery fact table. The pipeline then creates annual, monthly, and vehicle dimension tables.   

 

### EXTRA 
3. Set the GOOGLE_APPLICATION_CREDENTIALS appropriately</br>
   `export GOOGLE_APPLICATION_CREDENTIALS=/path/to/mage/google_cloud_key.json`

    Confirm it has been correctly set </br>
   `echo $GOOGLE_APPLICATION_CREDENTIALS`

4. Authenticate on GCloud </br>
   `gcloud auth application-default login`
