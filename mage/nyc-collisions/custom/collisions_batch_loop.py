import time
from mage_ai.orchestration.triggers.api import trigger_pipeline

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(input_object_keys, _, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    wait_seconds = 80 
    #input_object_keys = ['raw_api_batched/nyc_collisions_2020_09_.parquet', 'raw_api_batched/nyc_collisions_2020_10_.parquet']
    input_object_keys = sorted(input_object_keys)
    object_keys = input_object_keys
    for object_key in object_keys:
        trigger_pipeline(
            'collisions_process_batch',  
            variables={'object_key': object_key}, 
            check_status=False,
            error_on_failure=False,
            poll_interval=60,
            poll_timeout=None,
            schedule_name=None, 
            verbose=True,
        )
        print(f"Pipeline triggered for {object_key}. Waiting for next iteration.")
        time.sleep(wait_seconds)  
    return {}
    
@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
