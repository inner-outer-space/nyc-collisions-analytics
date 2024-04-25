import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import time
import calendar
import requests
import json
import os

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    """
    Extracts one month of data from the NYC Open Data API 
    to a df that is written to gcs as a parquet file
    
    kwargs:
        year (str): the year to extract data for
        month (str): the month to extract data for
        batch_size (int): the number of records to fetch in each API call
    
    returns: None 

    """
    collisions_df = pd.DataFrame()
    dfs = []
    
    year = year
    month_str = str(month).zfill(2)
    
    batch_size = batch_size
    offset = 0
    batch_num = 0
    
    # Define the base URL and query
    base_url = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json'
    query = f"$where=date_trunc_ym(crash_date) = '{year}-{month_str}'"
    query += f"&$limit={batch_size}"
        
    # Fetch JSON data from the API in reasonable sized batches
    while True:
        # Define the URL to fetch
        offset_query = f"&$offset={offset}"
        url = f'{base_url}?{query}{offset_query}'
        print(url)
        
        # Fetch the data
        response = requests.get(url)
        if not response.ok:
            raise RuntimeError(f"Failed to fetch data from API: {response.status_code}")

        # Normalize the json response 
        df = pd.json_normalize(response.json())
        print(df.shape)

        # Append the data to the list of dataframes
        dfs.append(df)
        
        # Check if there are more records to fetch
        if len(df) < batch_size:
            break
        
        offset += batch_size
        batch_num += 1

    collisions_df = pd.concat(dfs)
    collisions_df.reset_index(drop=True, inplace=True)    

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
