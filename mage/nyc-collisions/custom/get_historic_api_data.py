from mage_ai.orchestration.triggers.api import trigger_pipeline
import time

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
    for year in range(2013, 2014):
        for month in range(1, 13):
            month_str = str(month).zfill(2)
            trigger_pipeline(
                'monthly_extract_from_api',
                variables={
                    "year": f"{year}",
                    "month": f"{month}",
                    "output_parquet":f"nyc_collisions_{year}_{month_str}_.parquet"
                },
                check_status=False,
                error_on_failure=False,
                poll_interval=60,
                poll_timeout=None,
                schedule_name=None,  # Enter a unique name to create a new trigger each time
                verbose=True,
            )

        # Allow a break between calls
        time.sleep(30)
    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
