import os
from datetime import datetime
import pytz
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, TimestampType
from astral import LocationInfo
from astral.sun import sun

def compute_datetime(date, time):
    """Combine date and time strings into a datetime object."""
    return datetime.strptime(date + ' ' + time, '%Y/%m/%d %H:%M')

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

def main():
    # Set up Spark context and session
    jar_file = "/home/lulu/spark/spark-3.5.0-bin-hadoop3/lib/gcs-connector-hadoop3-latest.jar"
    credentials_location = '/home/lulu/projects/data_talks/mage-zoomcamp/google_cloud_key.json'
    
    #spark_home = os.environ.get('SPARK_HOME')
    #jar_file = os.path.join(spark_home, 'lib', 'gcs-connector-hadoop3-latest.jar')
    print(jar_file)

    spark = SparkSession.builder \
        .appName('ProcessCollisions') \
        .config("spark.jars", jar_file) \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
        .getOrCreate()  

    #sc = SparkContext(conf=conf)
    #spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

    # Read data
    df_collisions = spark.read.parquet('gs://collisions-first-try/collisions.parquet')

    # Register UDFs
    compute_datetime_udf = udf(compute_datetime, TimestampType())
    get_sun_phase_udf = udf(get_sun_phase, StringType())
    spark.udf.register("get_sun_phase", get_sun_phase)

    # Apply UDFs
    result_df = df_collisions \
        .withColumn("crash_datetime", compute_datetime_udf(col("crash_date"), col("crash_time"))) \
        .withColumn("sun_phase", get_sun_phase_udf(col("crash_datetime")))

    # Show results (for testing, consider saving results for production use)
    #result_df.select('crash_date','crash_time', 'sun_phase').show(20)
    output_folder = 'gs://collisions-first-try/collisions_with_sunphase.parquet'
    
    result_df.coalesce(1) \
    .write.parquet(output_folder, mode='overwrite')
if __name__ == "__main__":
    main()
