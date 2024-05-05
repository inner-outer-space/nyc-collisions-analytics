# nyc-collisions-analytics

This repository contains my final project for the [Data Engineering Zoomcamp by DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp). The course covered a varitey of data engineering topics and tools including docker, terraform, dbt, dlt, google cloud platform, spark, and kafka. <br/>

My course notes and homework can be found here: [DE Zoomcamp 2024 Repo](https://github.com/inner-outer-space/de-zoomcamp-2024). <br/>

To reproduce this project, please refer to the the [Set Up Instructions](https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/docs/setup_instructions.md)
<br/>

<div align = center>
  
### DASHBOARD
<img src=images/Collisions-Dashboard.png width="700" height="auto">
<br/>
[View Dashboard](https://lookerstudio.google.com/reporting/f738bd1c-d126-4e76-8c11-0446923e1a8e)
</div>
<br/>

### OBJECTIVE 
To apply the principles and techniques learned in the data engineering course by building an end-to-end cloud data pipeline for the NYC Collisions dataset. The project ingests, transforms, and enriches the data with local meterological data in monthly batches and visualizes the combined dataset.  

### DATA SOURCES 
The City of New York provides access to The [New York City Motor Vehicle Collisions Data Set](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) through the [NYC Open Data platform](https://opendata.cityofnewyork.us/). The dataset encompasses a wide range of collision types, including but not limited to, vehicle-to-vehicle crashes, pedestrian-involved incidents, and cyclist accidents. Key attributes include the date and time of the collision, location details such as the borough and street, as well as contributing factors and vehicle types involved.

As part of this excercise, the dataset will be enriched with meteorological data obtained from [World Weather Online](https://www.worldweatheronline.com/weather-api/). Additionally, the sun phase will be calculated based on the day of the year and latitude/longitude coordinates, providing further insights into environmental factors potentially influencing collision occurrences. This enriched dataset enables a more comprehensive analysis of traffic collisions in New York City.

<div align = center>

### ARCHITECTURE 
<img src="https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/images/architecture-diagram.png" width="700" height="auto">
</div>
<br/>

### Tools and Technologies
- Cloud - **Google Cloud Platform**
- Infrastructure as Code Software - **Terraform**
- Containerization - **Docker** and **Docker Compose**
- Orchestration - **Mage**
- Data Extract - **REST API**
- Transformation - **DBT & Apache Spark**
- Data Lake - **Google Cloud Storage**
- Data Warehouse - **BigQuery**
- Data Visualization - **Looker Studio**
- Languages - **Python, SQL, and Bash**
<br/>


## PROJECT WORKFLOW 

### 1. Google Cloud Platform Provisioning with Terraform
Terraform is used to: 
- Provision a GCP bucket and Bigquery dataset. 
- Enabel Google Cloud APIs. 
### 2. Containerization with Docker
Docker and Docker-Compose are used to create a local container environment for running Mage with integrated DBT and Spark.
### 3. Orchestration with Mage
Mage is employed for orchestrating the data pipeline, managing dependencies between tasks, and automating workflows.
### 4. Data Ingestion
Collision Data Ingestion 
  - The collision data is retrieved on a per month basis. 
  - Requests are made in batches until the full month of data has been retrieved (2-3 requests/ mos).     
  - The raw data is then written to the GCP bucket. 
  - The full historic dataset is retrieved by running a bash script that triggers the monthly extract pipeline for the full timeframe, with a wait is included between each month. <br/>
  
Weather Data Ingestion  
  - The data is retrieved and saved to CSV using a python script. Since the weather dataset is not publicly available, this step was done in order for this pipeline to be reproducible. 
  - The CSV file is ingested into Mage, where some light column name transformations are performed, and then written to the GCS bucket.

### 5. Data Processing with Spark
- Initial processing of the collisions dataset is handled using Spark.
Data types are assigned.
- A user-defined function (UDF) is applied to calculate the sun phase (day, night, dusk, or dawn) based on date and time.
### 6. Data Transformation with DBT
- The collision data is further transformed and enriched with weather data using DBT.
- Monthly and annual views are created as well as a vehicle view from the unnested data.
### 7. Visualization with Looker
- Looker is utilized for visualizing and analyzing the transformed data stored in BigQuery.


<div align = center>
  
## MAGE PIPELINES 
### Extract and Process Weather Data Pipeline <br/>
<img src=images/mage-weather.png width="400" height="auto">
<br/>
<br/>

### Extract Collisions Data Pipeline <br/>
<img src=images/mage-extract-collisions-monthly.png width="500" height="auto">
<br/>
<br/>

### Process Collisions Full Data Pipeline <br/>
<img src=images/mage-collisions-all.png width="500" height="auto">
<br/>
<br/>

### Process Collisions Batch Data Pipeline <br/>
This previous pipeline loops through a list of raw data files and calls this pipeline to process each file. 
<img src=images/mage-collisions-batch-process.png width="500" height="auto">
<br/>
<br/>

## DBT Lineage <br/>
<img src=images/dbt-lineage.png width="600" height="auto">
</div>


 
