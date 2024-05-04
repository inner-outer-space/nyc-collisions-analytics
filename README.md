# nyc-collisions-analytics

This repository contains my final project for the [Data Engineering Zoomcamp by DataTalksClub](https://github.com/DataTalksClub/data-engineering-zoomcamp). The course covered a varitey of data engineering topics and tools including docker, terraform, dbt, dlt, google cloud platform, spark, and kafka. <br/>

My course notes and homework can be found here: [DE Zoomcamp 2024 Repo](https://github.com/inner-outer-space/de-zoomcamp-2024). 


### OBJECTIVE 
To apply the principles and techniques learned in the data engineering course by building an end-to-end cloud data pipeline for the NYC Collisions dataset. The project ingests, transforms, and enriches the data with local meterological data in monthly batches and visualizes the combined dataset.  

### DATA SOURCES 
The City of New York provides access to a comprehensive collection of open data through the NYC Open Data platform (https://opendata.cityofnewyork.us/). The [New York City Motor Vehicle Collisions Data Set](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95), which is available on this platform, documents various types of traffic collisions reported within the city. The dataset encompasses a wide range of collision types, including but not limited to, vehicle-to-vehicle crashes, pedestrian-involved incidents, and cyclist accidents. Key attributes include the date and time of the collision, location details such as the borough and street, as well as additional factors contributing factors vehicle types involved.

As part of this excercise, the dataset will be enriched with meteorological data obtained from [World Weather Online](https://www.worldweatheronline.com/weather-api/). Additionally, the sun phase will be calculated based on the day of the year and latitude/longitude coordinates, providing further insights into environmental factors potentially influencing collision occurrences. This enriched dataset will enable a more comprehensive analysis of traffic collisions in New York City.

<div align = center>

### ARCHITECTURE 
<img src="https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/images/architecture-diagram.png" width="500" height="auto">
</div>
<br/>

<div align = center>

### DASHBOARD
<img src=images/Collisions-Dashboard.png width="500" height="auto">
</div>
<br/>


### SET UP 
To reproduce this project, please refer to the the [Set Up Instructions](https://github.com/inner-outer-space/nyc-collisions-analytics/blob/main/docs/setup_instructions.md)
<br/>

### Tools and Technologies
- Cloud - **Google Cloud Platform**
- Infrastructure as Code Software - **Terraform**
- Containerization - **Docker** and **Docker Compose**
- Orchestration - **Mage**
- Transformation - **DBT & Apache Spark**
- Data Lake - **Google Cloud Storage**
- Data Warehouse - **BigQuery**
- Data Visualization - **Looker Studio**
- Languages - **Python, SQL, and Bash**

### DATA SOURCES 
- [New York City Motor Vehicle Collisions Data Set](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) <br/>
  This data is available from Jan 2013 till present.
- [3-hr Meteorological Data Set from Weather API](https://www.worldweatheronline.com/weather-api/) <br/>
  This data was extracted via a REST API through with the free trial. I've posted the data in parquet format on my GIT for download into this pipeline.
  <details>
  <summary> Code for extracting the Weather Data </summary>  

  ```python
    """
    Description:
        This script fetches weather data from the World Weather Online API
        for each month within a specified range of years. A subset of the JSON response is aggregated into 
        a df and saved to a parquet in the same folder. 
        
    Usage:
        python weather_data.py <start_year> <end_year> <api_key>
    
        - start_year: The start year for fetching weather data (e.g., 2013).
        - end_year: The end year for fetching weather data (e.g., 2023).
        - api_key: Your API key for accessing the World Weather Online API.
    """
    
    import pandas as pd
    from datetime import datetime
    import requests
    from dateutil.relativedelta import relativedelta
    from dateutil.rrule import rrule, MONTHLY
    import sys
    
    def get_first_and_last_days(start_year, end_year):
        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31)
    
        first_days = list(rrule(freq=MONTHLY, dtstart=start_date, until=end_date))
        last_days = [day + relativedelta(months=1, days=-1) for day in first_days]
    
        return first_days, last_days
    
    def fetch_weather_data(start_year, end_year, key):
        first_days, last_days = get_first_and_last_days(start_year, end_year)
        base_url = 'http://api.worldweatheronline.com/premium/v1/past-weather.ashx'
        df_weather = pd.DataFrame()
    
        for start_date, end_date in zip(first_days, last_days):
            url = f'{base_url}?key={key}&q=New York City&format=json&extra=isDayTime&date={start_date}&enddate={end_date}&tp=1'
            response = requests.get(url)
    
            if not response.ok:
                raise Exception(f"Failed to fetch data from API: {response.status_code}")
    
            rows = []
    
            try:  
                for entry in response.json()['data']['weather']:
                    date = entry['date']
                    for hour in entry['hourly']:
                        row = {
                            'date': date,
                            'time': hour['time'],
                            'tempC': hour['tempC'],
                            'tempF': hour['tempF'],
                            'windspeedMiles': hour['windspeedMiles'],
                            'windspeedKmph': hour['windspeedKmph'],
                            'winddirDegree': hour['winddirDegree'],
                            'winddir16Point': hour['winddir16Point'],
                            'weatherCode': hour['weatherCode'],
                            'weatherDesc': hour['weatherDesc'][0]['value'],
                            'precipMM': hour['precipMM'],
                            'humidity': hour['humidity'],
                            'visibility': hour['visibility'],
                            'pressure': hour['pressure'],
                            'cloudcover': hour['cloudcover'],
                        }
                        rows.append(row)
            except KeyError:
                print("Response JSON:", response.json())
                raise
    
            df_batch = pd.DataFrame(rows)
            df_weather = pd.concat([df_weather, df_batch], ignore_index=True)
            df_weather.to_parquet('weather_data.parquet', index=False)
    
    if __name__ == "__main__":
        if len(sys.argv) != 4:
            print("Usage: python script_name.py <start_year> <end_year> <api_key>")
            sys.exit(1)
    
        start_year = int(sys.argv[1])
        end_year = int(sys.argv[2])
        key = sys.argv[3]
  ```
  <br>
  </details>

# PROJECT WORKFLOW 

##1. Cloud Platform 
##2. Infrastructure as Code: Terraform 
##3. Orchestration: Mage 
##4. Data Ingestion
##5. Data Processing 
##6. Data Load 
##7. Visualization





<div align = center>
  
## MAGE PIPELINES 
### Extract and Process Weather Data Pipeline <br/>
<img src=images/mage-weather.png width="300" height="auto">
<br/>

### Extract Collisions Data Pipeline <br/>
<img src=images/mage-extract-collisions-monthly.png width="300" height="auto">
<br/>

### Process Collisions Batch Data Pipeline <br/>
<img src=images/mage-collisions-batch-process.png width="300" height="auto">
<br/>

### Process Collisions Full Data Pipeline <br/>
<img src=images/mage-collisions-all.png width="300" height="auto">
<br/>

## DBT Lineage <br/>
<img src=images/dbt-lineage.png width="600" height="auto">
</div>


 
