import os 
from mage_ai.io.config import ConfigKey, EnvironmentVariableLoader
from mage_ai.io.config import ConfigKey, ConfigFileLoader

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """

    config = ConfigFileLoader('/home/src/nyc-collisions/io_config.yaml')
    postgres_db = config[ConfigKey.POSTGRES_DBNAME]
    print(postgres_db)
    for key, value in os.environ.items():
        print(f"{key}: {value}")
    print(os.environ['GOOGLE_BUCKET_NAME'])
    return {}



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'