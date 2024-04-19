# nyc-collisions-analytics

This repository contains my final project for the [Data Engineering Zoomcamp by DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp). In the course, we covered a wide varitey of data engineering topics and tools. This project aims to sythesize the information and put it to practice to create an automated data pipeline. 

## DATA SOURCES 
- [New York City Motor Vehicle Collisions Data Set](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) <br/>
  This data is available from Jan 2013 till present.
- [3-hr Meteorological Data Set from Weather API](https://www.worldweatheronline.com/weather-api/) <br/>
  This data was extracted via a REST API through with the free trial. I've posted the data in parquet format on my GIT for download into this pipeline. 

For my course notes and homework solutions, see my [DE Zoomcamp 2024 Repo](https://github.com/inner-outer-space/de-zoomcamp-2024). 

To reproduce this project, see the [Set Up Instructions](https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/docs/setup_instructions.md)

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

# ARCHITECTURE 
<img src="https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/images/architecture-diagram.png" width="600" height="auto">



## MAGE PIPELINES 
### Extract pipeline <br/>
<img src="https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/images/extract_mage.png" width="200" height="auto">

### Processing pipeline <br/>
<img src="https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/images/mage-tree.png" width="500" height="auto">

## DBT Lineage <br/>
<img src="https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/images/dbt-lineage.png" width="600" height="auto">



 
