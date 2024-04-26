from mage_ai.data_preparation.variable_manager import set_global_variable
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from google.cloud import storage
from io import BytesIO

import pandas as pd
import io
import os
from os import path
import requests
from datetime import datetime
import time
import pytz
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from pyspark.sql.functions import udf, col, date_format, to_date, to_timestamp, concat, lit

from astral import LocationInfo
from astral.sun import sun

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# DEFINE THE SUNPHASE UDF ##########################################
def get_sun_phase(timestamp):
    """Determine the sun phase based on timestamp."""
    date = timestamp.date()
    time = timestamp.time()
    city = LocationInfo("New York", "USA", "America/New_York", 40.730610, -73.935242)
    s = sun(city.observer, date=date, tzinfo=city.timezone)

    if s['sunrise'].time() <= time <= s['sunset'].time():
        return "day"
    elif s['dawn'].time() <= time <= s['sunrise'].time():
        return "dawn"
    elif s['sunset'].time() <= time <= s['dusk'].time():
        return "dusk"
    else:
        return "night"

# DATA CLEAN UP STEP - REMOVE NON INT VALUES FROM INT COLUMNS 
def remove_non_int(value):
    if value is None:
        return None
    # Check if the value is composed of digits only
    if str(value).isdigit():
        return int(value)
    else:
        return None

@custom
def gcs_to_spark_trans(*args, **kwargs):
    """
    Adust the datatypes of the columns and add sun phase based on a calculation
    """
    # Start timing
    start_time = time.time()
    
    # SparkSession - mage didnt consistently pass spark in the kwargs 
    spark = (
        SparkSession
        .builder
        .appName('Test spark')
        .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
        .config("spark.master", "local[*]")  # Use all available cores
        .config("spark.driver.memory", "4g")  # Set driver memory to 4GB  
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") # Set timeParserPolicy to LEGACY
        .getOrCreate()
    )

    print(spark.sparkContext.getConf().get("spark.jars"))
    print("Number of cores used:", spark.sparkContext.defaultParallelism)
    print("Driver memory:", spark.conf.get("spark.driver.memory"))

    # Register UDFs
    get_sun_phase_udf = udf(get_sun_phase, StringType())
    spark.udf.register("get_sun_phase", get_sun_phase)

    remove_non_int_udf = udf(remove_non_int, IntegerType())
    spark.udf.register("remove_non_int", remove_non_int)

    # Set GCS Variables
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = kwargs['key_path'] 
    bucket_name = kwargs['google_bucket']

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    object_key = 'raw_api_batched/nyc_collisions_2016_10_.parquet'
    
    # Extracting file name from object_key
    file_name = object_key.split('/')[-1]
    output_file_name = 'processed_' + file_name
    set_global_variable(kwargs['pipeline_uuid'], 'output_file_name', output_file_name)
    for key, value in kwargs.items():
        print (key, value)

    # Download the file contents as bytes
    blob = bucket.blob(object_key)
    file_contents = blob.download_as_string()

    # Read into pandas df and convrt to Spark df (i had trouble reading it directly into spark)
    df = pd.read_parquet(BytesIO(file_contents))
    spark_df = spark.createDataFrame(df)

    # Partition spark DF
    spark_df = spark_df.repartition(48)

    ###### CREATE DATETIME AND CORRECT COLUMN DATATYPES ###############

    spark_df = spark_df.withColumn("crash_date", to_date(col("crash_date"), "yyyy-MM-dd"))
    spark_df = spark_df.withColumn("crash_time", date_format(col("crash_time"), "HH:mm"))
    # Combine date and time into a single timestamp column to use in sun_phase calc
    spark_df = spark_df.withColumn("crash_timestamp", to_timestamp(concat(col("crash_date"), lit(" "), col("crash_time"))))
    
    spark_df = spark_df.withColumn("latitude", col("latitude").cast("double")) \
                .withColumn("longitude", col("longitude").cast("double"))

    # Columns to clean up and convert to Int 
    columns_to_cast = {
        #"number_of_persons_injured": IntegerType(),
        "number_of_pedestrians_injured": IntegerType(),
        "number_of_cyclist_injured": IntegerType(),
        "number_of_motorist_injured": IntegerType(),
        #"number_of_persons_killed": IntegerType(),
        "number_of_pedestrians_killed": IntegerType(),
        "number_of_cyclist_killed": IntegerType(),
        "number_of_motorist_killed": IntegerType(),
        "collision_id": IntegerType(),
        "zip_code": IntegerType(),
    }
        
    for col_name, col_type in columns_to_cast.items():
        spark_df = spark_df.withColumn(col_name, remove_non_int_udf(col(col_name)))
        spark_df = spark_df.withColumn(col_name, col(col_name).cast(IntegerType()))


    ###### ADD SUNPHASE #########################################
    spark_df = spark_df \
        .withColumn("sun_phase", get_sun_phase_udf(col("crash_timestamp")))

    ##### DROP COLUMNS ##########################################
    # going to add number of persons injured or killed back in as a calculation excercise in dbt. 
    columns_to_drop = ['location.human_address','location.latitude', 'location.longitude']
    #columns_to_drop = ['location.human_address','location.latitude', 'location.longitude','number_of_persons_injured','number_of_persons_killed']
    spark_df = spark_df.drop(*columns_to_drop)    

    ##### WRITE TO PARQUET FILES #################################
    spark_df = spark_df.coalesce(1)
    pandas_df = spark_df.toPandas()

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    output_object_key = 'crash_data_spark_trans/' + output_file_name

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        pandas_df,
        bucket_name,
        output_object_key,
    )   
    
    delta_t = time.time() - start_time
            
    spark.stop()
    return output_file_name


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
