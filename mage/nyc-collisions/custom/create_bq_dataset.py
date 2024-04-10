from google.cloud import bigquery
from google.cloud.exceptions import NotFound

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

    # Construct a bigquery client object.
    client = bigquery.Client()

    # Set your dataset details
    dataset_id = "nyc-auto-accidents.nyc_collisions"

    # Try to get the dataset to check if it exists
    try:
        dataset = client.get_dataset(dataset_id)  # Make an API request.
        print(f"Dataset {dataset_id} already exists.")
    except NotFound:
        print(f"Dataset {dataset_id} is not found. Creating it now.")
        dataset = bigquery.Dataset(dataset_id)

        # Specify the geographic location where the dataset should reside
        dataset.location = "EU"

        # Send the dataset creation request
        try:
            dataset = client.create_dataset(dataset)  # Make an API request.
            print(f"Dataset {dataset_id} created.")
        except Conflict:
            print(f"Dataset {dataset_id} already exists.")

    return dataset_id

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
