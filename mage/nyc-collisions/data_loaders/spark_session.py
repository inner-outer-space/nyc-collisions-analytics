from pyspark.sql import SparkSession

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
        .config("spark.master", "local[8]")  # Use all available cores
        #.config("spark.executor.memory", "2g")  # For Distributed Mode
        #.config("spark.executor.cores", "4") # For Distributed Mode
        .config("spark.driver.memory", "4g")  # Set driver memory to 4GB  
        .getOrCreate()
    )
    
    kwargs['context']['spark'] = spark

    print(spark.sparkContext.getConf().get("spark.jars"))
    print("Number of cores used:", spark.sparkContext.defaultParallelism)
    #print("Executor memory:", spark.conf.get("spark.executor.memory"))
    #print("Executor cores:", spark.conf.get("spark.executor.cores"))
    print("Driver memory:", spark.conf.get("spark.driver.memory"))

    return {}

@test
def test_output(output, *args, **kwargs) -> None:
    """
    Template code for testing the output of the block.
    """
    assert kwargs['spark'] is not None, 'The output is undefined'
