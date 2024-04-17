from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
from pyspark.sql.functions import udf, col, date_format, to_date, to_timestamp, concat, lit

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def spark_cast_dtypes(data, *args, **kwargs):
    """
    Updates column dtypes and constructs crash_datetime from date and time columns.  

    Args:
        data: The output from the upstream parent block

    Returns:
        spark data frame 
    """
    for key, value in kwargs.items():
        print(f"{key}: {value}")
    spark = kwargs['spark']
    
    # Fix the time columns
    data = data.withColumn("crash_date", to_date(col("crash_date"), "yyyy-MM-dd"))
    data = data.withColumn("crash_time", date_format(col("crash_time"), "HH:mm"))
    # Combine date and time into a single timestamp column
    data = data.withColumn("crash_datetime", to_timestamp(concat(col("crash_date"), lit(" "), col("crash_time"))))

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
        data = data.withColumn(col_name, col(col_name).cast(col_type))

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
