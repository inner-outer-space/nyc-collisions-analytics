from datetime import datetime, timedelta
import os
from os import path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import time
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def api_to_gcs_by_month(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    # Define the project, bucket, and target folder  
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = kwargs['key_path']
    project_id = kwargs['google_project_id']
    bucket_name = kwargs['google_bucket']
    target_folder = kwargs['google_gcs_raw']
    
    # Define the base URL
    base_url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?"

    # Define the start and end dates
    start_date = datetime(2017, 1, 1)
    end_date = datetime(2023, 12, 31)

    # Loop through each month from Jan 2017 to Dec 2023
    date_range_urls = []
    while start_date <= end_date:
        # Calculate the end date of the month
        next_month = start_date.replace(day=28) + timedelta(days=4)
        end_of_month = next_month - timedelta(days=next_month.day)

        # Extract month and year
        month_year = start_date.strftime("%B %Y")
        
        # Construct the URL for the current month
        url = f"{base_url}$where=crash_date between '{start_date.isoformat()}' and '{end_of_month.isoformat()}'"
        response = requests.get(url)
        
        if not response.ok:
            print(f"Failed to fetch data from API: {response.status_code}")
            break

        # Normalize the json response 
        df = pd.json_normalize(response.json())

        # define the pyarrow table and read the df into it
        pa_table = pa.Table.from_pandas(df)

        # define file syste 
        gcs = pa.fs.GcsFileSystem()
        
        output_path = f'{bucket_name}/{target_folder}/nyc_collisions_{month_year}.parquet'

        # API response is partitioned by year and written to gcs as parquet
        pq.write_table(
            pa_table, 
            output_path,
            filesystem=gcs
        )

        print(f'Month: {month_year} done')
        
        # Move to the next month
        start_date = next_month

        return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
