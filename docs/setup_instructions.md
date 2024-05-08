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

## Terraform
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
<br/>

## Mage 
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
<br/>
<br/>

# RUNNING THE PIPELINES 

### EXTRACT, PROCESS, AND STAGE THE WEATHER DATA (2 min)
The weather data from World Weather Online used in this project is not publically available. The data was extracted from the REST API and stored in CSV format for use in this pipeline. This pipeline retrieves the CSV file from its GIT location, does some light processing, uploads it to GCS, and then creates an associated external table that can be accessed in BigQuery.  <br/> <br/>

1. Goto `http://localhost:6789/pipelines/weather_extract_and_prep_full_data/triggers`
2. Click `Run@once` and then `Run now`

### EXTRACT THE HISTORIC COLLISIONS DATA (~36 min)
This pipeline extracts the historical collision data from the REST API. It loops through the months within 2015 - 2023 and triggers the 'collisions_extract_monthly_from_api' pipeline for each month. The monthly pipeline makes batch requests to the NYC Open Data rest api until the full month of data is retrieved and then ouputs each month's data as a parquet to GCS. The script includes a 30s pause between each month to be respectful of the source.

1. Goto `http://localhost:6789/pipelines/collisions_extract_all/triggers`
2. Click `Run@once` and then `Run now`

### PROCESS COLLISION DATA AND INCORMPORATE WEATHER (90 min)
This pipeline processes the collisions data, enriches it with the weather data, and then creates annual, monthly, and vehicle dimension tables. The pipeline loops through the list of the monthly raw files and triggers the collisions_process_batch pipeline for each file.  Local spark is used to create a datetime stamp, assign data types, and calculate the sun phase (day, dusk, dawn, dark) at the time of each collision. DBT is used to further transform the collision data and join it with the weather data. The final data is incrementally added to the BigQuery fact table. The pipeline then creates annual, monthly, and vehicle dimension tables.   

1. Goto `http://localhost:6789/pipelines/collisions_process_all/triggers`
2. Click `Run@once` and then `Run now`

### VIEW THE DATA IN BIG QUERY 
![image](https://github.com/inner-outer-space/nyc-collisions-analytics/assets/12296455/f7a8627b-e669-4be7-acf1-b84813b3ed95)

## TAKING THE PROJECT DOWN 
1. Go to the Mage folder and execute `docker-compose down'
2. Go to the Terraform folder and execute `terraform destroy` to take down the resources.
3. Once the files and resources are removed, you can delete the project.  

