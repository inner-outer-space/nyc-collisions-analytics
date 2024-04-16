import pandas as pd
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
    object_key = 'ny_collision_data/0576784114e14db79eb8b50814139cc8-0.parquet'

    df = GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).load(
        bucket_name,
        object_key,
    )
    df.head()

    small_df = df[['crash_datetime']]
    smaller_df = small_df.head(5)
    #df = df.withColumn("crash_datetime", col("crash_datetime").cast("timestamp"))
    print(small_df.dtypes)
    #small_df = df[['crash_datetime']].head(5)
    #small_df['crash_datetime'] = pd.to_datetime(small_df['crash_datetime'])

    return small_df




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
