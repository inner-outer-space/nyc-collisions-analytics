import time
from mage_ai.data_preparation.variable_manager import set_global_variable
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def add_wait(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    #output_file_name = args[0]
    #set_global_variable(kwargs['pipeline_uuid'], 'output_file_name', output_file_name)
    print('kwargs output file name', kwargs['output_file_name'])  

    # Wait so that the file has time to show up in gcs. 
    # Ohterwise create ext table will fail. 
    time.sleep(30) 


    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
