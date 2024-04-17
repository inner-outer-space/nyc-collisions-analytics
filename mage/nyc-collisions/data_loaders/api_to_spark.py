
import os
import requests
from os import path
import pandas as pd
import time
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from pyspark.sql.functions import udf, col, date_format, to_date, to_timestamp, concat, lit

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def fetch_and_save_batch_from_api(*args, **kwargs):

    spark = kwargs['spark']

    # Start timing
    start_time = time.time()

    offset = 0
    batch_num = 0
    batch_size = 25000
    df = []

    #api_endpoint = 'https://df.cityofnewyork.us/resource/h9gi-nx95.json'
    api_endpoint = 'https://data.cityofnewyork.us/resource/h9gi-nx95.json'

    custom_schema = StructType([
    StructField('collision_id', StringType(), True),
    StructField('crash_date', TimestampType(), True),
    StructField('crash_time', TimestampType(), True),
    StructField('zip_code', StringType(), True),
    StructField('borough', StringType(), True),
    StructField('latitude', StringType(), True), 
    StructField('longitude', StringType(), True),
    StructField('on_street_name', StringType(), True),
    StructField('off_street_name', StringType(), True),
    StructField('cross_street_name', StringType(), True),
    StructField('number_of_persons_injured', StringType(), True),
    StructField('number_of_pedestrians_injured', StringType(), True),
    StructField('number_of_cyclist_injured', StringType(), True),
    StructField('number_of_motorist_injured', StringType(), True),
    StructField('number_of_persons_killed', StringType(), True),
    StructField('number_of_pedestrians_killed', StringType(), True),
    StructField('number_of_cyclist_killed', StringType(), True),
    StructField('number_of_motorist_killed', StringType(), True),
    StructField('contributing_factor_vehicle_1', StringType(), True),
    StructField('contributing_factor_vehicle_2', StringType(), True),
    StructField('contributing_factor_vehicle_3', StringType(), True),
    StructField('contributing_factor_vehicle_4', StringType(), True),
    StructField('contributing_factor_vehicle_5', StringType(), True),
    StructField('vehicle_type_code1', StringType(), True),
    StructField('vehicle_type_code2', StringType(), True),
    StructField('vehicle_type_code_3', StringType(), True),
    StructField('vehicle_type_code_4', StringType(), True),
    StructField('vehicle_type_code_5', StringType(), True)
    ])

    # Fetch JSON df from the API in batches
    while True:

        # RETRIEVE DATA FROM API ENDPOINT 
        url = f'{api_endpoint}?$limit={batch_size}&$offset={offset}'
        delta_t = time.time() - start_time
        print(f'got url {delta_t}')
        
        response = requests.get(url)
        delta_t = time.time() - start_time
        print(f'got response {delta_t}')
        if not response.ok:
            raise Exception(f"Failed to fetch df from API: {response.status_code}")

        # Extract JSON content
        json_content = response.json()
        delta_t = time.time() - start_time
        print(f'got json content {delta_t}')
        # Convert JSON content to RDD
        rdd = spark.sparkContext.parallelize(json_content)
        delta_t = time.time() - start_time
        print(f'rdd {delta_t}')
        
        # Read JSON df into DataFrame
        df = spark.read.json(rdd, schema=custom_schema)

        delta_t = time.time() - start_time
        print(f'df {delta_t}')

        # Partition spark DF
        df = df.repartition(24)

        ###### CORRECT THE COLUMN DATATYPES AND ADD DATETIME #####
        # Fix the time columns
        df = df.withColumn("crash_date", to_date(col("crash_date"), "yyyy-MM-dd"))
        df = df.withColumn("crash_time", date_format(col("crash_time"), "HH:mm"))
        # Combine date and time into a single timestamp column
        df = df.withColumn("crash_datetime", to_timestamp(concat(col("crash_date"), lit(" "), col("crash_time"))))

        columns_to_cast = {
            "latitude": DoubleType(),
            "longitude": DoubleType(),
            "number_of_persons_injured": IntegerType(),
            "number_of_pedestrians_injured": IntegerType(),
            "number_of_cyclist_injured": IntegerType(),
            "number_of_motorist_injured": IntegerType(),
            "number_of_persons_killed": IntegerType(),
            "number_of_pedestrians_killed": IntegerType(),
            "number_of_cyclist_killed": IntegerType(),
            "number_of_motorist_killed": IntegerType(),
            "collision_id": IntegerType(),
            "zip_code": IntegerType(),
        }
            
        for col_name, col_type in columns_to_cast.items():
            df = df.withColumn(col_name, col(col_name).cast(col_type))
        delta_t = time.time() - start_time
        print(f'dtypes {delta_t}')

        # Collapse the partitions before writing to parquet
        df = df.coalesce(1)
        delta_t = time.time() - start_time
        print(f'coalesce {delta_t}')
        # Convert to pandas to use the gcs loader
        pandas_df = df.toPandas()

        config_path = path.join(get_repo_path(), 'io_config.yaml')
        config_profile = 'default'

        bucket_name = 'collisions-first-try'
        object_key = f'spark_transformed_data/collisions_batch_{batch_num}.parquet'

        GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
            pandas_df,
            bucket_name,
            object_key,
        )
        delta_t = time.time() - start_time
        print(f'write to GCS {delta_t}')
        delta_t = time.time() - start_time
        print (f'Batch #: {batch_num} complete')
        #if batch_num == 2:

        print(f'offset {offset} df length {pandas_df.shape[0]}')
        if batch_size > pandas_df.shape[0]:
            break
        
        offset += batch_size
        batch_num += 1

        # Wait between API calls
        # time.sleep(60)

    # Stop SparkSession
    spark.stop()
    
    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'