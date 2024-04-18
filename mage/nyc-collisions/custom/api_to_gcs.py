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

# Set the environment variable to the location of the mounted key. json
# This will tell pyarrow where our credentials are
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/google_cloud_key.json"

# Define the project, bucket, and target folder  
project_id = 'ny-auto-accidents'
bucket_name = 'collisions-first-try'
target_folder = 'raw_api_batched'

@custom
def api_to_gcs_partitioned(*args, **kwargs):

    collisions_df = pd.DataFrame()
    offset = 0
    batch_size = 25000
    batch_num = 0

    #api_endpoint = 'https://df.cityofnewyork.us/resource/h9gi-nx95.json'
    api_endpoint = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json'

    # Fetch JSON data from the API in batches
    while True:
        print('start')
        url = f'{api_endpoint}?$limit={batch_size}&$offset={offset}'
        print(url)
        response = requests.get(url)
        print('response received')
        
        if not response.ok:
            print(f"Failed to fetch data from API: {response.status_code}")
            break

        # There are 2 nested layers. Normalize will take care of the first nested layer, 
        df = pd.json_normalize(response.json())

        # the location nested json is deleted immediately as it is not needed.
        #if 'location.human_address' in df.columns:
        #    df.drop(columns = ['location.human_address','location.latitude', 'location.longitude'], inplace=True)

        # define the pyarrow table and read the df into it
        pa_table = pa.Table.from_pandas(df)

        # define file syste - the google cloud object that is going to authorize using the environmental variable automatically
        gcs = pa.fs.GcsFileSystem()
        
        output_path = f'{bucket_name}/{target_folder}/nyc_collisions_batch_{batch_num}.parquet'
        print(output_path)

        # API response is partitioned by year and written to gcs as parquet
        pq.write_table(
            pa_table, 
            output_path,
            filesystem=gcs
        )

        # Check if there are more records to fetch
        if len(df) < batch_size:
        #if offset > 100000:
            break
        
        offset += batch_size
        batch_num += 1
        print(f'{batch_num} done')
        time.sleep(10)

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
