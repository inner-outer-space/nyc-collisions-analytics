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
