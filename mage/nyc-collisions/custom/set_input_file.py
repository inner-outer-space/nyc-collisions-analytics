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
    object_key = 'raw_api_batched/nyc_collisions_2017_09_.parquet'
    #object_key = kwargs.get('object_key')
    print(object_key)

    # Extracting file name from object_key
    file_name = object_key.split('/')[-1]
    print(f'file name {file_name}')

    output_file_name = 'processed_' + file_name
    print(f'output_file_name {output_file_name}')

   #output_file_name = args[0]
    set_global_variable(kwargs['pipeline_uuid'], 'output_file_name', output_file_name)
    print('kwargs', kwargs['output_file_name'])  

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
