from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from mage_ai.data_preparation.variable_manager import set_global_variable

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    schema = StructType([
        types.StructField('collision_id',types.IntegerType(), True),
        types.StructField('crash_date',types.TimestampType(), True),
        types.StructField('crash_time',types.StringType(), True),
        types.StructField('zip_code',types.DoubleType(), True),
        types.StructField('borough',types.StringType(), True),
        types.StructField('latitude',types.DoubleType(), True),
        types.StructField('longitude',types.DoubleType(), True),
        types.StructField('on_street_name',types.StringType(), True),
        types.StructField('off_street_name',types.StringType(), True),
        types.StructField('cross_street_name',types.StringType(), True),
        types.StructField('number_of_persons_injured',types.DoubleType(), True),
        types.StructField('number_of_pedestrians_injured',types.IntegerType(), True),
        types.StructField('number_of_cyclist_injured',types.IntegerType(), True),
        types.StructField('number_of_motorist_injured',types.IntegerType(), True),
        types.StructField('number_of_persons_killed',types.DoubleType(), True),
        types.StructField('number_of_pedestrians_killed',types.IntegerType(), True),
        types.StructField('number_of_cyclist_killed',types.IntegerType(), True),
        types.StructField('number_of_motorist_killed',types.IntegerType(), True),
        types.StructField('contributing_factor_vehicle_1',types.StringType(), True),
        types.StructField('contributing_factor_vehicle_2',types.StringType(), True),
        types.StructField('contributing_factor_vehicle_3',types.StringType(), True),
        types.StructField('contributing_factor_vehicle_4',types.StringType(), True),
        types.StructField('contributing_factor_vehicle_5',types.StringType(), True),
        types.StructField('vehicle_type_code1',types.StringType(), True),
        types.StructField('vehicle_type_code2',types.StringType(), True),
        types.StructField('vehicle_type_code_3',types.StringType(), True),
        types.StructField('vehicle_type_code_4',types.StringType(), True),
        types.StructField('vehicle_type_code_5',types.StringType(), True),
    ])
    holy = 9

    set_global_variable(kwargs['pipeline_uuid'], 'spark_schema', holy)

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
