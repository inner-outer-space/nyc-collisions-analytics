import transforms as transforms
import pandas as pd
import geopandas as gpd
import time
from datetime import datetime, date

def main():
    start_time = time.time()  # Record the start time
    start_time_local = datetime.datetime.fromtimestamp(start_time)
    print('Started date load at: ', start_time_local) 
    
    # Read data
    data_location = '../data/ny_collisions/Motor_Vehicle_Collisions_-_Crashes_20240326.csv'
    collisions_df = pd.read_csv(data_location, low_memory=False)
    
    # Preprocess data
    preprocess_data(collisions_df)
    
    # Spatial operations
    perform_spatial_operations(collisions_df)
    
    collisions_df.to_parquet('data_from_script.parquet')
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time

    print(f"Script execution took {elapsed_time:.2f} seconds")

def preprocess_data(df):
    # Standardize column names
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    
    # Convert crash_date and crash_time to datetime
    df['Datetime'] = pd.to_datetime(df['crash_date'] + ' ' + df['crash_time'], format='%m/%d/%Y %H:%M')
    df['crash_date'] = pd.to_datetime(df['crash_date'], format="%m/%d/%Y").dt.date
    df['crash_time'] = pd.to_datetime(df['crash_time'], format="%H:%M").dt.time
    
    # Extract datetime components
    df['crash_month'] = df['Datetime'].dt.month
    df['crash_year'] = df['Datetime'].dt.year
    df['crash_day'] = df['Datetime'].dt.day
    df['crash_weekday'] = df['Datetime'].dt.weekday
    df['crash_hour'] = df['Datetime'].dt.hour
    df['crash_month_year'] = df['Datetime'].dt.to_period('M')
    
    # Determine sun phase
    df['sun_phase'] = df.apply(lambda row: transforms.nyc_sunphase(row['crash_date'], row['crash_time']), axis=1)

def perform_spatial_operations(df):
    # Read GeoJSON file
    polygon_file = '/home/lulu/projects/data_talks/data/ny_collisions/nyc_borough_neighborhood.geojson'
    gdf = gpd.read_file(polygon_file)
    
    # Fill missing borough values
    df['borough'] = df.apply(lambda x: transforms.lon_lat_to_borough(x['longitude'], x['latitude'], gdf)
                            if pd.isna(x['borough']) else x['borough'].lower().title(), axis=1)
    
    # Add neighborhood column
    df['neighborhood'] = df.apply(lambda x: transforms.lon_lat_to_neighborhood(x['longitude'], x['latitude'], gdf), axis=1)

if __name__ == "__main__":
    main()