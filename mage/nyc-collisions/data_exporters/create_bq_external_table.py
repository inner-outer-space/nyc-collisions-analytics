from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def create_external_table_in_big_query(**kwargs):
    print("Positional arguments:", args)
    print("Keyword arguments:", kwargs)
    # Creating external table referring to gcs path
    #query = """
    #CREATE OR REPLACE EXTERNAL TABLE `nyc-auto-accidents.nyc-auto-accidents.external_data`
    #OPTIONS (
    ##format = 'parquet',
    #uris = ['gs://collisions-first-try/ny_collision_data'/*/*]
    #);
    #"""
    #config_path = path.join(get_repo_path(), 'io_config.yaml')
    #config_profile = 'default'
    #BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).execute(query)
