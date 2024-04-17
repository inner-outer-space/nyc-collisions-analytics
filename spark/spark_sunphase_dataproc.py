# # EXAMPLE FROM CLI DATAPROC 
# gcloud dataproc jobs submit pyspark \
#     --cluster=collisions-dp-cluster\
#     --region=europe-west1 \
#     gs://collisions-first-try/spark_code/spark_sunphase_dataproc.py \
#     -- \
#         --input_folder=gs://collisions-first-try/ny_collision_data/ \
#         --output=gs://collisions-first-try/sunsphase_output/

#CREATE DATA PROC CLUSTER COMMAND LINE 
#gcloud dataproc clusters create nyc-collisions-dataproc --region europe-west6 --no-address --single-node --master-machine-type e2-standard-2 --master-boot-disk-type pd-balanced --master-boot-disk-size 250 --image-version 2.2-ubuntu22 --max-idle 3600s --initialization-actions 'gs://collisions-first-try/spark_code/install-python-packages.sh' --project nyc-auto-accidents

# PROBLEMS WITH DATAPROC 
# Can't install packages because I cant access the internet. 
# the cluster should have outgoing internet access. I even added an egress firewall rule to the network but still no internet access.
# Going to look into installing spark on a vm instead. 



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


# Add argparse and parse command line arguments
parser = argparse.ArgumentParser()

parser.add_argument('--input_folder', required=True)
parser.add_argument('--output_folder', required=True)

args = parser.parse_args()

input_file = args.input_folder
output_folder = args.output_folder

# Add function to compute datetime
def compute_datetime(date, time):
    """Combine date and time strings into a datetime object."""
    return datetime.strptime(date + ' ' + time, '%Y/%m/%d %H:%M')

# Add function to determine sun phase
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

# Add main function to add sun phase data to collisions data
def main():
    # Initialize Spark
    conf = SparkConf() \
        .setAppName('ProcessCollisions') \
        .getOrCreate()

    # Read data
    df_collisions = spark.read.parquet(input_folder)

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
    
    result_df.coalesce(1) \
    .write.parquet(output_folder, mode='overwrite')

if __name__ == "__main__":
    main()
