import pytest
import duckdb
import json

from digital_land.configuration.main import Config
from digital_land.organisation import Organisation
from digital_land.expectations.commands import run_dataset_checkpoint
import logging


@pytest.fixture
def test_organisations(mocker):
    # mock the init so nothing is loaded
    mocker.patch("digital_land.organisation.Organisation.__init__", return_value=None)
    # create class
    organisations = Organisation()

    # set up mocked org data, can be changed if the data underneath changes
    org_data = {
        "local-authority:test": {
            "entity": 101,
            "name": "test",
            "prefix": "local-authority",
            "reference": "test",
            "dataset": "local-authority",
            "organisation": "local-authority:test",
        },
        "local-authority:test_2": {
            "entity": 102,
            "name": "test_2",
            "prefix": "local-authority",
            "reference": "test_2",
            "dataset": "local-authority",
            "organisation": "local-authority:test_2",
        },
    }

    org_lookups = {
        "local-authority:test": "local-authority:test",
        "local-authority:test_2": "local-authority:test_2",
    }

    organisations.organisation = org_data
    organisations.organisation_lookup = org_lookups
    return organisations


def test_run_dataset_checkpoint_success(
    sqlite3_with_entity_tables_path, test_organisations, tmp_path, mocker
):
    """
    Test running the command to run the dataset checkpoint, uses a random
    operation as an examplel
    """
    # define rules to run
    rules = [
        {
            "datasets": "test",
            "organisations": "local-authority:test",
            "operation": "test",
            "params": {"test": "test"},
            "name": "a test expectation",
            "desription": "a test decription",
            "severity": "notice",
            "responsibility": "internal",
        }
    ]

    # mock config as it's hard to use at the minute without serious input
    mock_config = mocker.Mock(spec=Config)
    mock_config.get_expectation_rules.return_value = rules

    # mock operation so a simple operation can be used
    mock_function = mocker.Mock(
        return_value=(True, "this operation is a test and always passes", {})
    )  # Configure the mock to return 10

    # Mock the operation_factory to return the mock_function
    mocker.patch(
        "digital_land.expectations.checkpoints.dataset.DatasetCheckpoint.operation_factory",
        return_value=mock_function,
    )

    run_dataset_checkpoint(
        file_path=sqlite3_with_entity_tables_path,
        output_dir=tmp_path / "log/expectations",
        dataset="test",
        config=mock_config,
        organisations=test_organisations,
        act_on_critical_error=False,
    )

    assert (
        tmp_path / "log/expectations" / "dataset=test" / "test.parquet"
    ).exists(), "the expected log parquet file doesn't exist"

    # use duckdb to read in logs fromt the directory and test results
    # this ensures the output can be queried by duck db and the expected
    # logs are there
    conn = duckdb.connect()

    # Query the partitioned directory of Parquet files
    query = f"""
        SELECT *
        FROM read_parquet('{str(tmp_path / 'log/expectations' / '**/*.parquet')}');
    """

    # Execute the query
    cursor = conn.execute(query)

    # Get column names from the cursor
    columns = [desc[0] for desc in cursor.description]

    # Fetch all results as a list of dictionaries
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    logging.debug(f"The expectation log produced:\n{json.dumps(results, indent=4)}")
    assert len(results) == 1
    assert results[0]["organisation"] == "local-authority:test"
    assert results[0]["dataset"] == "test"
    assert False
