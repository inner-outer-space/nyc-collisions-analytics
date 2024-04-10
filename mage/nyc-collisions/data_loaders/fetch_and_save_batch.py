import os
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def fetch_and_save_batch_from_api(*args, **kwargs):

    collisions_df = pd.DataFrame()
    offset = 0
    batch_size = 200000
    batch_num = 0

    api_endpoint = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json'
    
    # Directory where batch files will be saved
    save_to_path = '/home/src/data/batches'

    # Check if the directory exists
    if not os.path.exists(save_to_path):
    # If the directory does not exist, create it
        os.makedirs(save_to_path)

    # Fetch JSON data from the API in batches
    while True:
        
        url = f'{api_endpoint}?$limit={batch_size}&$offset={offset}'
        print(url)
        response = requests.get(url)
        
        if not response.ok:
            print(f"Failed to fetch data from API: {response.status_code}")
            break

        # There are 2 nested layers. Normalize will take care of the first nested layer, 
        df = pd.json_normalize(response.json())

        # the second is not needed and will be deleted.
        if 'location.human_address' in df.columns:
            df.drop(columns = ['location.human_address','location.latitude', 'location.longitude'], inplace=True)
    
        # Modify the data types 
        column_dtypes = {
        'on_street_name': 'string',
        'off_street_name': 'string',
        'number_of_persons_injured': 'Int64',
        'number_of_persons_killed': 'Int64',
        'number_of_pedestrians_injured': 'Int64',
        'number_of_pedestrians_killed': 'Int64',
        'number_of_cyclist_injured': 'Int64',
        'number_of_cyclist_killed': 'Int64',
        'number_of_motorist_injured': 'Int64',
        'number_of_motorist_killed': 'Int64',
        'contributing_factor_vehicle_1': 'string',
        'contributing_factor_vehicle_2': 'string',
        'collision_id': 'Int64',
        'vehicle_type_code1': 'string',
        'vehicle_type_code2': 'string',
        'borough': 'string',
        'zip_code': 'string',
        'latitude': 'float64',
        'longitude': 'float64',
        'cross_street_name': 'string',
        'contributing_factor_vehicle_3': 'string',
        'vehicle_type_code_3': 'string',
        'contributing_factor_vehicle_4': 'string',
        'vehicle_type_code_4': 'string',
        'contributing_factor_vehicle_5': 'string',
        'vehicle_type_code_5': 'string',
        }

        df = df.astype(column_dtypes)

        # correct the formatting for the date time columns 
        df['crash_date'] = pd.to_datetime(df['crash_date']).dt.strftime('%Y/%m/%d')
        df['crash_time'] = pd.to_datetime(df['crash_time']).dt.strftime("%H:%M")
        df['crash_datetime'] = pd.to_datetime(df['crash_date'] + ' ' + df['crash_time'], format='%Y/%m/%d %H:%M')
        df['crash_month_year'] = df['crash_datetime'].dt.strftime('%Y-%m')
        df['crash_month']= df['crash_datetime'].apply(lambda x: x.month)
        df['crash_year']= df['crash_datetime'].apply(lambda x: x.year)
        df['crash_day']= df['crash_datetime'].apply(lambda x: x.day)
        df['crash_weekday']= df['crash_datetime'].apply(lambda x: x.weekday())
        df['crash_hour']= df['crash_datetime'].apply(lambda x: x.hour)

        # Data is writen to local storage
        output_path = f'{save_to_path}/ny_collisions_batch_{batch_num}.parquet'
        df.to_parquet(output_path)

        # Check if there are more records to fetch
        if len(df) < batch_size:
        #if offset > 200000:
            break
        
        offset += batch_size
        batch_num += 1

    return save_to_path


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
