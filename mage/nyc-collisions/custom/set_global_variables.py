from mage_ai.data_preparation.variable_manager import set_global_variable
import os

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def set_global_variables(*args, **kwargs):
    """
    Set global variables programatically using env variables.
    This makes it easier for someone to reproduce with their own gcs setup. 
    """

    set_global_variable('nyc_collisions_enriched_full_pipeline', 'google_bucket', os.environ['GOOGLE_BUCKET_NAME'])
    set_global_variable('nyc_collisions_enriched_full_pipeline', 'google_project_id', os.environ['GOOGLE_PROJECT_ID'])
    set_global_variable('nyc_collisions_enriched_full_pipeline', 'google_raw_folder', os.environ['GOOGLE_BUCKET_FOLDER'])
    set_global_variable('nyc_collisions_enriched_full_pipeline', 'key_path', os.environ['GOOGLE_SERVICE_ACC_KEY_FILEPATH'])

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
