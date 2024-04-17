if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import os
from datetime import datetime
import pytz
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
#from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, TimestampType
from astral import LocationInfo
from astral.sun import sun

# DEFINE THE SUNPHASE FUNCTION 
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

@transformer
def transform(data, *args, **kwargs):
    spark = kwargs['spark']

    df_collisions = spark.createDataFrame(data)

    # Convert the crash_datetime column to TimestampType
    df_collisions = df_collisions.withColumn("crash_datetime", col("crash_datetime").cast("timestamp"))

    # Register UDFs
    get_sun_phase_udf = udf(get_sun_phase, StringType())
    spark.udf.register("get_sun_phase", get_sun_phase)

    # Apply UDFs
    result_df = df_collisions \
        .withColumn("sun_phase", get_sun_phase_udf(col("crash_datetime")))
    
    result_df.head()
    return result_df




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
