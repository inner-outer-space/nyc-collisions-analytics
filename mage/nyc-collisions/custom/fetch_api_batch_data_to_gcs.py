import os
from os import path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame


if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Set the environment variable to the location of the mounted key. json
# This will tell pyarrow where our credentials are
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/google_cloud_key.json"

# Define the bucket, project, and table  
project_id = 'nyc-auto-accidents'
bucket_name = 'collisions-first-try'
folder_name = 'ny_collision_data'

root_path = f'{bucket_name}/{folder_name}'

@custom
def fetch_and_save_batch_from_api(*args, **kwargs):

    collisions_df = pd.DataFrame()
    offset = 0
    batch_num = 0
    batch_size = 200000
    

    api_endpoint = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json'

    # Fetch JSON data from the API in batches
    while True:
        
        # RETRIEVE DATA FROM API ENDPOINT 
        url = f'{api_endpoint}?$limit={batch_size}&$offset={offset}'
        print(url)
        response = requests.get(url)
        
        if not response.ok:
            raise Exception(f"Failed to fetch data from API: {response.status_code}")

        # TRANSFORM THE DATA 
        # There are 2 nested layers. Normalize will take care of the first nested layer, 
        df = pd.json_normalize(response.json())

        # the second level of nested data is not needed. It is deleted immediately to save space. 
        if 'location.human_address' in df.columns:
            df.drop(columns = ['location.human_address','location.latitude', 'location.longitude'], inplace=True)

        # check the length of date values
        df['date_len'] = df['crash_date'].apply(lambda x: len(x))
        print(df['date_len'].value_counts())
        
        df.drop(columns='date_len', inplace = True)
        # Transform the time columns that are easier to handle in python
        #df['crash_date'] = pd.to_datetime(df['crash_date']).dt.strftime('%Y/%m/%d')
        #df['crash_time'] = pd.to_datetime(df['crash_time']).dt.strftime("%H:%M")
        #df['crash_datetime'] = pd.to_datetime(df['crash_date'] + ' ' + df['crash_time'], format='%Y/%m/%d %H:%M')
        #df['crash_year'] = df['crash_datetime'].dt.year

        # WRITE THE BATCHES TO GCS 
        # convert to a PyArrow table
        table = pa.Table.from_pandas(df)

        # define file syste - the google cloud object that is going to authorize using the environmental variable automatically
        gcs = pa.fs.GcsFileSystem()

        # write to the dataset using a parquet function
        pq.write_to_dataset(
            table, 
            root_path=root_path, 
            filesystem=gcs
        )
               
         #    partition_cols=['crash_year'], # needs to be a list

        # Check if there are more records to fetch
        #if len(df) < batch_size:
        if batch_num == 2:
        #if offset > 200000:
            break
        
        offset += batch_size
        batch_num += 1


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
