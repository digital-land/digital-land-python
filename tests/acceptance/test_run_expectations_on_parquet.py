"""
A set of tests to mimic a user (computational or otherwise) running tests against
a parquet dataset.
"""
# import os.path

import pytest
import csv
import duckdb
import logging
import json
import numpy as np
import pandas as pd
import glob
import sqlite3
import os
from pathlib import Path
from collections import defaultdict


from click.testing import CliRunner

from digital_land.cli import expectations_run_dataset_checkpoint
from digital_land.configuration.main import Config
from digital_land.specification import Specification


@pytest.fixture
def dataset_path(tmp_path):
    dataset_path = tmp_path / "test.duckdb"

    test_entity_data = pd.DataFrame.from_dict(
        {
            "entity": [1],
            "name": ["test1"],
            "organisation_entity": [122],
            "geometry": [
                "MULTIPOLYGON(((-0.4914554581046105 53.80708847427775,-0.5012039467692374 53.773842823566696,-0.4584064520895481 53.783669118729875,-0.4914554581046105 53.80708847427775)))"  # noqa E501
            ],
            "point": ["POINT(-0.4850078825017034 53.786407721600625)"],
        }
    )
    test_old_entity_data = pd.DataFrame.from_dict({"old_entity": [100], "entity": [10]})
    # define rows to load in these have been prebuilt to fit into a certain area
    # you can view the WKTs at .... they are designed to fit in the lpa boundary  below
    conn = duckdb.connect(dataset_path)
    test_entity_data.to_sql('entity', conn, if_exists='append', index=False)
    test_old_entity_data.to_sql('old_entity', conn, if_exists='append', index=False)
    conn.close()

    ##########################################################################
    # Need to check if it's better to store these as duckdb or parquet files #
    # If using parquet files use the code below and delete above lines       #
    ##########################################################################
    # if os.path.exists(f"{tmp_path}/entity.parquet"):
    #     tmp_df = pd.read_parquet(f"{tmp_path}/entity.parquet")
    #     test_entity_data = pd.concat([tmp_df, test_entity_data], ignore_index=True)
    # test_entity_data.to_parquet(f"{tmp_path}/entity.parquet", index=False)
    # if os.path.exists(f"{tmp_path}/old_entity.parquet"):
    #     tmp_df = pd.read_parquet(f"{tmp_path}/old_entity.parquet")
    #     test_old_entity_data = pd.concat([tmp_df, test_old_entity_data], ignore_index=True)
    # test_old_entity_data.to_parquet(f"{tmp_path}/old_entity.parquet", if_exists="append", index=False)

    return dataset_path


@pytest.fixture
def organisation_path(tmp_path):
    """
    build an organisations dataset to use
    """
    orgs_path = tmp_path / "organisation.csv"
    orgs = [
        {
            "entity": 122,
            "name": "test",
            "prefix": "local-authority",
            "reference": "test",
            "dataset": "local-authority",
            "organisation": "local-authority:test",
        },
        {
            "entity": 102,
            "name": "test_2",
            "prefix": "local-authority",
            "reference": "test_2",
            "dataset": "local-authority",
            "organisation": "local-authority:test_2",
        },
    ]

    with open(orgs_path, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=orgs[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(orgs)

    return orgs_path


@pytest.fixture
def config_path(tmp_path, specification_dir):
    """create a configuration to use"""
    config_path = tmp_path / "config.duckdb"
    rules = [
        {
            "datasets": "test",
            "organisations": "local-authority:test",
            "name": "test rule for {{ organisation.name }}",
            "operation": "count_lpa_boundary",
            "parameters": '{"expected":1,"lpa":"{{ organisation.local_planning_authority }}","organisation_entity":"{{ organisation.entity }}"}',
            "responsibility": "internal",
            "severity": "notice",
        }
    ]
    # write rows
    expect_path = tmp_path / "expect.csv"
    # Writing data to CSV
    with open(expect_path, mode="w", newline="") as file:
        # Define the fieldnames (column headers) based on dictionary keys
        writer = csv.DictWriter(file, fieldnames=rules[0].keys())
        writer.writeheader()  # Write the header row
        writer.writerows(rules)  # Write each dictionary as a row

    spec = Specification(specification_dir)
    config = Config(path=config_path, specification=spec)
    config.create()
    config.load({"expect": str(tmp_path)})
    return config_path


def test_run_some_expectations(
        tmp_path, organisation_path, dataset_path, config_path, specification_dir, mocker
):
    # set up inputs
    dataset = "test"

    # # set up mocking of API response, we don't want to be dependent on the data returned
    # # this could be lifted in the future if we just want to ensure it runs even if the test fails
    # lpa_geometry = "MULTIPOLYGON(((-0.49901924973862233 53.81622315189787,-0.5177418530633007 53.76114469621959,-0.4268378912177833 53.78454002743749,-0.49901924973862233 53.81622315189787)))"  # noqa E501
    # mock_response = mocker.Mock()
    # mock_response.status_code = 200
    # mock_response.json.return_value = {
    #     "geometry": lpa_geometry,
    # }
    #
    # # Mock the `requests.Session.get` method
    # mocker.patch("requests.get", return_value=mock_response)
    # # set up cli runner
    # runner = CliRunner()
    #
    # result = runner.invoke(
    #     expectations_run_dataset_checkpoint,
    #     [
    #         "--dataset",
    #         dataset,
    #         "--file-path",
    #         str(dataset_path),
    #         "--log-dir",
    #         str(tmp_path / "log"),
    #         "--configuration-path",
    #         str(config_path),
    #         "--organisation-path",
    #         str(organisation_path),
    #         "--specification-dir",
    #         str(specification_dir),
    #     ],
    #     catch_exceptions=False,
    # )
    #
    # # Check that the command exits with status code 0 (success)
    # if result.exit_code != 0:
    #     # Print the command output if the test fails
    #     print("Command failed with exit code:", result.exit_code)
    #     print("Command output:")
    #     print(result.output)
    #     print("Command error output:")
    #     print(result.exception)
    #
    # assert result.exit_code == 0, "error returned when running expectations"
    # assert (
    #     tmp_path / f"log/expectation/dataset={dataset}/{dataset}.parquet"
    # ).exists(), "no logs created"

    # # logs are there
    # conn = duckdb.connect()
    #
    # # Query the partitioned directory of Parquet files
    # query = f"""
    #     SELECT *
    #     FROM read_parquet('{str(tmp_path / 'log/expectation' / '**/*.parquet')}');
    # """
    #
    # # Execute the query
    # cursor = conn.execute(query)
    #
    # # Get column names from the cursor
    # columns = [desc[0] for desc in cursor.description]
    #
    # # Fetch all results as a list of dictionaries
    # results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    # logging.debug(f"The expectation log produced:\n{json.dumps(results, indent=4)}")
    # # we need to check that a log was made when running the above as we inputted a test
    # assert len(results) > 0, "check that some logs were created"
    # # we need to check for some specific user required fields in the output
    # # these have been confirmed as required outputs for any test
    # required_fields = [
    #     "passed",
    #     "name",
    #     "details",
    #     "parameters",
    #     "organisation",
    #     "dataset",
    # ]
    # for field in required_fields:
    #     assert field in results[0].keys(), f"field : {field} not found in log"
    # assert (
    #     len([result for result in results if result["passed"] == "True"]) == 1
    # ), "expected 1 passed expectation"

    output_dir = "tmp_path/output/"
    parquet_files = glob.glob(f"{output_dir}*/*.parquet")
    assert len(parquet_files) > 0, "No parquet files produced"
    # Ensure the same number of parquet files in each directory
    file_counts = defaultdict(int)
    # Walk through all subdirectories and count files
    for root, dirs, files in os.walk(output_dir):
        subdir = os.path.relpath(root, output_dir)
        if subdir != '.':
            subdir = os.path.basename(root)
            pq_files = [file for file in files if file.endswith(".parquet")]
            file_counts[subdir] = len(pq_files)
    unique_counts = set(file_counts.values())
    assert len(unique_counts) == 1, "Different number of parquet files in each subdirectory"

    sql_files = glob.glob(f"{output_dir}*/*.sqlite3")
    pq_dir_names = np.unique([os.path.basename(os.path.dirname(file)) for file in parquet_files])
    sql_file_names = [Path(os.path.basename(file)).stem for file in sql_files]
    assert len(pq_dir_names) == len(sql_file_names), \
        "Mismatch between number of sql files and number of directories for parquet files"
    assert np.all(np.sort(pq_dir_names) == np.sort(sql_file_names)), \
        "Mismatch between names of sql files and names of directories for parquet files"

    for pq_dir in pq_dir_names:
        pq_dir_files = [file for file in parquet_files if os.path.dirname(file).endswith(pq_dir)]
        sql_file = [file for file in sql_files if Path(os.path.basename(file)).stem == pq_dir]
        cnx = sqlite3.connect(sql_file[0])
        sqlite_tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", cnx)
        for pq_file in pq_dir_files:
            df_pq = pd.read_parquet(pq_file)
            table_name = Path(os.path.basename(pq_file)).stem
            assert np.any(sqlite_tables['name'] == table_name), \
                f"Table {table_name} is  not in the {pq_dir} database"
            df_sql = pd.read_sql_query(f"SELECT * FROM {table_name}", cnx)
            assert len(df_pq) == len(df_sql), \
                f"Mismatch between rows in {table_name} in the {pq_dir} directory"
        cnx.close()


