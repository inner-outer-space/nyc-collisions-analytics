# nyc-collisions-analytics

This repository contains my final project for the [Data Engineering Zoomcamp by DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp). The course covered a varitey of data engineering topics and tools. My course notes and homework can be found here: [DE Zoomcamp 2024 Repo](https://github.com/inner-outer-space/de-zoomcamp-2024). 

This project aims to sythesize the information and put it to practice to create an automated end-to-end data pipeline utilizing Docker, Terraform, Spark, Google Cloud Platform (GCP), and Mage as an Orchestrator. 

## DATA SOURCES 
- [New York City Motor Vehicle Collisions Data Set](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) <br/>
  This data is available from Jan 2013 till present.
- [3-hr Meteorological Data Set from Weather API](https://www.worldweatheronline.com/weather-api/) <br/>
  This data was extracted via a REST API through with the free trial. I've posted the data in parquet format on my GIT for download into this pipeline. 

To reproduce this project, see the [Set Up Instructions](https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/docs/setup_instructions.md)

### Tools and Technologies

- Cloud - **Google Cloud Platform**
- Infrastructure as Code software - **Terraform**
- Containerization - **Docker** and **Docker Compose**
- Orchestration - **Mage**
- Transformation and Batch Processing - **Apache Spark**
- Data Lake - **Google Cloud Storage**
- Data Warehouse - **BigQuery**
- Data Visualization - **Looker Studio**
- Languages - Python, SQL, and Bash

# ARCHITECTURE 
<img src="https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/images/architecture-diagram.png" width="600" height="auto">



## MAGE PIPELINES 
### Extract pipeline <br/>
<img src="https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/images/extract_mage.png" width="200" height="auto">

### Processing pipeline <br/>
<img src="https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/images/mage-tree.png" width="500" height="auto">

## DBT Lineage <br/>
<img src="https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/images/dbt-lineage.png" width="600" height="auto">



 
