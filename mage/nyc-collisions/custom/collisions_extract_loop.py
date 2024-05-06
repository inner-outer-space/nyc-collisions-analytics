import time
from mage_ai.orchestration.triggers.api import trigger_pipeline

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def collisions_extract_loop(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Define start and end years
    start_year = 2015
    end_year = 2023

    # Generate list of tuples (year, month) for each month between start and end years
    all_months = [(year, month) for year in range(start_year, end_year + 1) for month in range(1, 13)]

    base_url="http://localhost:6789/api/pipeline_schedules/21/pipeline_runs/81ec754a748a453681dd30d3e12e3f1d"

    wait_seconds = 20

    # Iterate over all year-month tuples
    for year, month in all_months:
        trigger_pipeline(
            'collisions_extract_monthly_from_api',  
            variables={
                'year': year, 
                'month': month
            }, 
            check_status=False,
            error_on_failure=False,
            poll_interval=60,
            poll_timeout=None,
            schedule_name=None, 
            verbose=True,
        )
        print(f"Pipeline triggered for {month}-{year}. Waiting for next iteration.")
        time.sleep(wait_seconds)  
    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
