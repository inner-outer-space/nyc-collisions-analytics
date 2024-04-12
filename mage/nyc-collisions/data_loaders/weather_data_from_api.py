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
    Template for loading data from API
    """
    url = 'http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=bf2b1f005ae542e89b1123837241204 &q=New York City&format=json&extra=isDayTime&date=2013-01-01&enddate=2024-03-31'
    response = requests.get(url)
    
    if not response.ok:
        print(f"Failed to fetch data from API: {response.status_code}")
        break

    # TRANSFORM THE DATA 
    # There are 2 nested layers. Normalize will take care of the first nested layer, 
    df = pd.json_normalize(response.json())

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
