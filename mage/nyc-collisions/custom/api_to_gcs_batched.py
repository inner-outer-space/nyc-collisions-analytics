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

@custom
def api_to_gcs_partitioned(*args, **kwargs):

    collisions_df = pd.DataFrame()
    offset = 0
    batch_size = 20000
    batch_num = 0

    #api_endpoint = 'https://df.cityofnewyork.us/resource/h9gi-nx95.json'
    api_endpoint = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json'


    # Define the project, bucket, and target folder  
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = kwargs['key_path']
    project_id = kwargs['google_project_id']
    bucket_name = kwargs['google_bucket']
    target_folder = kwargs['google_gcs_raw']

    # Fetch JSON data from the API in reasonable sized batches
    while True:
        url = f'{api_endpoint}?$limit={batch_size}&$offset={offset}'
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
        
        output_path = f'{bucket_name}/{target_folder}/nyc_collisions_batch_{batch_num}.parquet'

        # API response is partitioned by year and written to gcs as parquet
        pq.write_table(
            pa_table, 
            output_path,
            filesystem=gcs
        )

        # Check if there are more records to fetch
        if len(df) < batch_size:
            break
        
        offset += batch_size
        batch_num += 1

        print(f'Batch: {batch_num} done')

        # Allow a break between calls
        time.sleep(10)

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
