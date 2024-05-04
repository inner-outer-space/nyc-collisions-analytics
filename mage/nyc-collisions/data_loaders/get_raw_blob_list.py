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
    # Set Google Env Variables 
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = kwargs['key_path']
    bucket_name = kwargs['google_bucket']
    target_folder = kwargs['google_gcs_raw']
    folder_path= f'{target_folder}/'

    # Retrieve list of raw api response files in GCS 
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=folder_path)
  
    input_object_keys = [blob.name for blob in blobs if blob.name.endswith('.parquet')]
    #print(len(input_object_keys))
    #print(input_object_keys[24:30])
    return input_object_keys


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
