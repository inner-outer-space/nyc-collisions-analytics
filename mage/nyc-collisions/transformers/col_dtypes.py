import pandas as pd
from datetime import datetime 
import dask.dataframe as dd
import os

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test




@transformer
def transform(data_folder, *args, **kwargs):
    """
    Args:
        df df from last block

    Returns:
        df with updated df types 
    """

    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        print(file_path)
        df = pd.read_parquet(file_path)

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

        #print(df.dtypes)
        df.to_parquet(file_path, index=False)

    return data_folder


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
