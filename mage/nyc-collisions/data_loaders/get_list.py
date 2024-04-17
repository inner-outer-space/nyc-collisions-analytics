from google.cloud import storage
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    """
    Template for loading data from a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'collisions-first-try'
    #object_key = 'your_object_key'

    # Create a Google Cloud Storage client
    client = storage.Client()
    
    # Get the bucket
    bucket = client.get_bucket(bucket_name)
    
    # List all objects in the bucket
    blobs = bucket.list_blobs()
    
    # Extract the names of the files
    file_names = [blob.name for blob in blobs]

    return file_names


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
