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


@custom
def load_from_gcs_to_spark(*args, **kwargs):
    """
    DESCRIPTION
    """
    # Start timing
    start_time = time.time()
    
    # SparkSession 
    spark = kwargs['spark']
    # Register UDFs
    get_sun_phase_udf = udf(get_sun_phase, StringType())
    spark.udf.register("get_sun_phase", get_sun_phase)

    # Set GCS Variables 
    bucket_name = kwargs['google_bucket']

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    input_object_keys = args[0]
    # Loop over the parquet files to process the data
    for batch_num, object_key in enumerate(input_object_keys):
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
        
        # Set dtype of other columns
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
            spark_df = spark_df.withColumn(col_name, col(col_name).cast(col_type)) 

        ###### ADD SUNPHASE #########################################
        spark_df = spark_df \
            .withColumn("sun_phase", get_sun_phase_udf(col("crash_timestamp")))

        ##### DROP COLUMNS ##########################################
        #columns_to_drop = ['crash_date', 'crash_time', 'location.human_address','location.latitude', 'location.longitude']
        columns_to_drop = ['location.human_address','location.latitude', 'location.longitude']
        spark_df = spark_df.drop(*columns_to_drop)    

        ##### WRITE TO PARQUET FILES #################################
        spark_df = spark_df.coalesce(1)
        pandas_df = spark_df.toPandas()

        config_path = path.join(get_repo_path(), 'io_config.yaml')
        config_profile = 'default'

        output_object_key = f'crash_data_spark_trans/collisions_batch_{batch_num}.parquet'

        GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
            pandas_df,
            bucket_name,
            output_object_key,
        )   
        
        delta_t = time.time() - start_time
        print(f'batch {batch_num} done at: {delta_t}')
        
        if batch_num == 1:
            break
    spark.stop()
    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
