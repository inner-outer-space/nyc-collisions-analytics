from mage_ai.data_preparation.decorators import data_loader
from google.cloud import storage

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def list_files_in_gcs_bucket(*args, **kwargs):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'collisions-first-try'

    # Create a Google Cloud Storage client
    client = storage.Client()
    
    # Get the bucket
    bucket = client.get_bucket(bucket_name)
    
    # List all objects in the bucket
    blobs = bucket.list_blobs()
    
    # Extract the names of the files
    file_names = [blob.name for blob in blobs]
    
    print(file_names)

    return file_names


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
