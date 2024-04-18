import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Load weather data from a file stored in github. 
    Data was previously retrieved for Jan 2013 - Mar 2024 from a paid REST API service. 
    """
    url = 'https://github.com/inner-outer-space/nyc-collisions/raw/main/mage/weather_data.parquet'
    df = pd.read_parquet(url)
    
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
