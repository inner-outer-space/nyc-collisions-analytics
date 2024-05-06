import pandas as pd
import numpy as np  
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
import transforms
import geopandas as gpd
import matplotlib.pyplot as plt
import datetime
#from datetime import datetime, date

def main():
    start_time = time.time()  # Record the start time
    start_time_local = datetime.datetime.fromtimestamp(start_time)
    print('Started processing data at: ', start_time_local) 
    
    # define the path to the parquet file
    data_folder = '../data/crash_records/'
    output_folder = '../data/crash_records_transformed/'
    
    # READ IN DATA using PyArrow 
    try:
        dataset = pq.ParquetDataset(data_folder)
        table = dataset.read()
        raw_df = table.to_pandas()
    except Exception as e:
        print(f"Error loading data: {e}")
    
    # PROCESS DATA
    processed_df = process_data(raw_df)
    
    # WRITE DATA USING PYARROW 
    table = pa.Table.from_pandas(processed_df)
    pq.write_to_dataset(table, root_path=output_folder, partition_cols=['crash_year','crash_month','borough'])
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time

    print(f"Script execution took {elapsed_time:.2f} seconds")

def process_data(input_df):
    """
    Preprocess the data by performing the following steps:
        - Format column names
        - Drop unnecessary columns
        - Drop duplicates
        - Convert columns to appropriate data types
        - Add additional time columns (month, year, day, weekday, hour, month_year)
        - Convert lat/lon 0 to NaN
        - Determine sun phase
        
    Args:
        df (pd.DataFrame): Input DataFrame containing the raw data.

    Returns:
        pd.DataFrame: Preprocessed DataFrame.
    """
    df = input_df.copy()
    
    # Format column names 
    df.columns = df.columns.str.strip()
    
    # Drop unnecessary columns
    drop_cols =['location.human_address', 'location.longitude', 'location.latitude']
    df.drop(drop_cols, axis=1, inplace=True)
    
    # DROP DUPLICATES 
    # Sort the DataFrame based on collision_id in ascending order
    df.sort_values(by='collision_id', ascending=True, inplace=True)

    # Identify duplicates and mark all but the last occurrence as True
    duplicates_mask = df.duplicated(subset='collision_id', keep='first')

    # Filter out the rows marked as duplicates
    df = df[~duplicates_mask]
    
    # DATATYPES AND NULL VALUES
    # Define cols that are floats, ints, and strings
    # file NaN with 0 for int cols except for collision_id which should be unique
    float_cols = ['latitude','longitude']
    int_cols_no_fill = ['collision_id']
    int_cols_fill_0 = [
        'number_of_persons_injured','number_of_pedestrians_injured','number_of_cyclist_injured',
        'number_of_motorist_injured','number_of_persons_killed','number_of_pedestrians_killed',
        'number_of_cyclist_killed','number_of_motorist_killed'
    ]
    str_cols = [
        'zip_code','borough','on_street_name','off_street_name','cross_street_name',
        'contributing_factor_vehicle_1','contributing_factor_vehicle_2','contributing_factor_vehicle_3',
        'contributing_factor_vehicle_4','contributing_factor_vehicle_5','vehicle_type_code1',
        'vehicle_type_code2','vehicle_type_code_3','vehicle_type_code_4','vehicle_type_code_5'
]

    # Convert cols to appropriate dtypes
    df[float_cols] = df[float_cols].astype(float)
    df[int_cols_no_fill] = df[int_cols_no_fill].astype(int)
    df[int_cols_fill_0] = df[int_cols_fill_0].fillna(0).astype(int)
    df[str_cols] = df[str_cols].astype(str)
    
    # PROCESS TIME COLUMNS 
    df['crash_date'] = pd.to_datetime(df['crash_date'], format="%Y-%m-%dT%H:%M:%S.%f").dt.date
    df['crash_datetime'] = pd.to_datetime((df['crash_date'].astype(str) + ' ' + df['crash_time'].astype(str)), format='%Y-%m-%d %H:%M')
    
    df['crash_month']= df['crash_datetime'].dt.month
    df['crash_year']= df['crash_datetime'].dt.year
    df['crash_day']= df['crash_datetime'].dt.day
    df['crash_weekday']= df['crash_datetime'].dt.weekday
    df['crash_hour']= df['crash_datetime'].dt.hour
    df['crash_month_year'] = df['crash_datetime'].dt.strftime('%Y-%m')
    
    # Convert lat/lon 0 to NaN
    df.loc[df['latitude'] == 0, 'latitude'] = np.nan 
    df.loc[df['longitude'] == 0, 'longitude'] = np.nan 
    
    # ADD SEVERITY COLUMN
    # fatal = >0 deaths, severe = >0 injuries, minor = 0 deaths and injuries
    df['severity'] = np.where(df['number_of_persons_killed'] > 0, 'fatal',
                            np.where(df['number_of_persons_injured'] > 0, 'major', 'minor'))
    
    # ADD SUNPHASE COLUMN
    df['sun_phase'] = df.apply(lambda row: transforms.nyc_sunphase(row['crash_datetime']), axis=1)

    # SPATIAL OPERATIONS TO FILL IN BOROUGH AND NEIGHBORHOOD 
    # Read GeoJSON file
    polygon_file = '../data/nyc_borough_neighborhood.geojson'
    gdf = gpd.read_file(polygon_file)
    
    # Fill missing borough values
    df['borough'] = df.apply(lambda x: transforms.lon_lat_to_borough(x['longitude'], x['latitude'], gdf)
                            if pd.isna(x['borough']) else x['borough'].lower().title(), axis=1)
    
    # Add neighborhood column
    df['neighborhood'] = df.apply(lambda x: transforms.lon_lat_to_neighborhood(x['longitude'], x['latitude'], gdf), axis=1)

    return df

if __name__ == "__main__":
    main()