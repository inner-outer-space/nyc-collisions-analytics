from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
import os
from os import path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# Set the environment variable to the location of the mounted key. json
# This will tell pyarrow where our credentials are
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/google_cloud_key.json"

# Define the bucket, project, and table  
bucket_name = 'collisions-first-try'
project_id = 'nyc-auto-accidents'
table_name = 'ny_collision_data'


root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data_to_google_cloud_storage(data_folder, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    for filename in os.listdir(data_folder):
        file_path = os.path.join(data_folder, filename)
        print(file_path)
        df = pd.read_parquet(file_path)
    
        # define the pyarrow table and read the df into it
        table = pa.Table.from_pandas(df)

        # define file syste - the google cloud object that is going to authorize using the environmental variable automatically
        gcs = pa.fs.GcsFileSystem()

        # write to the dataset using a parquet function
        pq.write_to_dataset(
            table, 
            root_path=root_path, 
            partition_cols=['crash_year'], # needs to be a list
            filesystem=gcs
        )
