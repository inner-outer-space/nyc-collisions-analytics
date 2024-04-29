import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
from datetime import datetime, timedelta
import time
import calendar
import requests
import json
import os

def main(): 
    parser = argparse.ArgumentParser(description='Ingest NYC Collisions data from api to local parquet files')
    parser.add_argument('--batch_size', type=int, default=100000, help='batch size')
    parser.add_argument('--target_folder', type=str, default="../data/crash_records", help='target folder')
    parser.add_argument('--test', action='store_true', help='test flag')
    parser.add_argument('--verbose', action='store_true', help='verbose flag')
    
    args=parser.parse_args()
    
    extract_collisions_from_api_to_local(args.batch_size, args.target_folder, args.test, args.verbose)
    

    
def extract_collisions_from_api_to_local(batch_size=100000, target_folder="../data/crash_records", test = False, verbose = False):
    """
    Extracts all data from the NYC Open Data API in small batches and writes them to a local parquet file.
    There is a 5 second break between each batch to avoid overloading the API.
    """
    collisions_df = pd.DataFrame()
    offset = 0
    batch_size = batch_size
    batch_num = 0
    
    ##### DATA EXTRACTION #####
    # Define the base URL
    base_url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

    # Define the target folder  
    if test:
        target_folder = f'{target_folder}/test' 
    else:
        target_folder = target_folder
        
    if not os.path.exists(target_folder):
        os.makedirs(target_folder) 
    
    # Fetch JSON data from the API in reasonable sized batches
    while True:
        url = f'{base_url}?$limit={batch_size}&$offset={offset}'
        response = requests.get(url)

        if not response.ok:
            raise RuntimeError(f"Failed to fetch data from API: {response.status_code}")

        # Normalize the json response 
        df = pd.json_normalize(response.json())

        # define the pyarrow table and read the df into it
        pa_table = pa.Table.from_pandas(df)
        
        output_file = f'{target_folder}/nyc_collisions_batch_{batch_num}.parquet'

        # Save the data to a parquet file
        pq.write_table(pa_table, output_file, compression='None')
        
        if verbose:
            print(f'batch: {batch_num} Total records collected: {offset + batch_size}')
        
        # Break if test flag is set
        if test:
            if batch_num == 1:
                break
            
        # Check if there are more records to fetch
        if len(df) < batch_size:
            break
        
        # Update the offset and batch number
        offset += batch_size
        batch_num += 1

        # Allow a break between calls
        time.sleep(5)

# The parser is used to parse the command line arguments which are then passed to the main method.
if __name__ == '__main__':
    main()