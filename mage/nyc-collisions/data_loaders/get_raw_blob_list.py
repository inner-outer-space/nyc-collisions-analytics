from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from google.cloud import storage
from io import BytesIO

import pandas as pd
import io
import os
from os import path
import requests
import time

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Set Google Env Variable 
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/google_cloud_key.json"
    
    # Retrieve list of raw api response files in GCS 
    bucket_name = 'collisions-first-try'
    folder_path= 'raw_api_batched/'

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=folder_path)
  
    input_object_keys = [blob.name for blob in blobs if blob.name.endswith('.parquet')]
    #print(input_object_keys)
    return input_object_keys


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
