from mage_ai.data_preparation.variable_manager import set_global_variable
from pyspark.sql import SparkSession
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):

    #spark.stop()
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

    kwargs['context']['spark'] = spark
    #set_global_variable('nyc_collisions_enriched_full_pipeline', 'spark', spark)


    print(spark.sparkContext.getConf().get("spark.jars"))
    print("Number of cores used:", spark.sparkContext.defaultParallelism)
    print("Driver memory:", spark.conf.get("spark.driver.memory"))

    return {}

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'