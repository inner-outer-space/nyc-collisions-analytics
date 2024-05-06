from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    exports dataframe as a parquet file 

    args: df from previous block
    output: none 
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    year = kwargs['year']
    month = kwargs['month']

    month_str = str(month).zfill(2)
    output_parquet = f'nyc_collisions_{year}_{month_str}.parquet'
    print(output_parquet)
    
    bucket_name =kwargs['google_bucket']
    object_key = f"{kwargs['google_gcs_raw']}/{output_parquet}"

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
