from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    spark = kwargs['spark']
    custom_schema = StructType([
        StructField('collision_id', TimestampType(), True),
        StructField('crash_datetime', TimestampType(), True),
        StructField('crash_date', TimestampType(), True),
        StructField('crash_time', TimestampType(), True),
        StructField('crash_year', TimestampType(), True),
        StructField('zip_code', IntegerType(), True),
        StructField('borough', StringType(), True),
        StructField('latitude', DoubleType(), True),
        StructField('longitude', DoubleType(), True),
        StructField('on_street_name', StringType(), True),
        StructField('off_street_name', StringType(), True),
        StructField('cross_street_name', StringType(), True),
        StructField('number_of_persons_injured', IntegerType(), True),
        StructField('number_of_pedestrians_injured', IntegerType(), True),
        StructField('number_of_cyclist_injured', IntegerType(), True),
        StructField('number_of_motorist_injured', IntegerType(), True),
        StructField('number_of_persons_killed', IntegerType(), True),
        StructField('number_of_pedestrians_killed', IntegerType(), True),
        StructField('number_of_cyclist_killed', IntegerType(), True),
        StructField('number_of_motorist_killed', IntegerType(), True),
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

    print(spark.sparkContext.getConf().get("spark.jars"))
    print("Number of cores used:", spark.sparkContext.defaultParallelism)
    print("Executor memory:", spark.conf.get("spark.executor.memory"))
    print("Executor cores:", spark.conf.get("spark.executor.cores"))
    print("Driver memory:", spark.conf.get("spark.driver.memory"))

    spark_df = spark.read.schema(custom_schema).parquet("gs://collisions-first-try/ny_collision_data/")
    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
