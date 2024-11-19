"""
A set of tests to mimic a user (computational or otherwise) running tests against
a sqlite dataset. There are quite a few things to set up and this specifically
"""

import pytest
import csv

# import logging
# import json
import numpy as np
import pandas as pd
import os
import sqlite3

from click.testing import CliRunner

from digital_land.cli import cli
from digital_land.configuration.main import Config
from digital_land.specification import Specification


@pytest.fixture
def input_paths():
    input_paths = []
    directory = "tests/data/conservation-area/transformed/conservation-area/"
    for root, dirs, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(root, file)
            input_paths.append(full_path)

    return input_paths


@pytest.fixture
def organisation_path(tmp_path):
    """
    build an organisations dataset to use
    """
    orgs_path = "tests/data/organisation.csv"

    return orgs_path


@pytest.fixture
def config_path(tmp_path, specification_dir):
    """create a configuration to use"""
    config_path = tmp_path / "config.sqlite3"
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


@pytest.fixture
def cache_path(tmp_path):
    cache_path = tmp_path / "var/cache"
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
    return cache_path


def test_run_some_expectations(
    tmp_path, organisation_path, input_paths, config_path, specification_dir, cache_path
):
    # set up inputs
    dataset = "acceptance_tests_output"

    # # set up mocking of API response, we don't want to be dependent on the data returned
    # # this could be lifted in the future if we just want to ensure it runs
    # # even if the test fails
    # lpa_geometry = "MULTIPOLYGON(((-0.49901924973862233 53.81622315189787,-0.5177418530633007 53.76114469621959,-0.4268378912177833 53.78454002743749,-0.49901924973862233 53.81622315189787)))"  # noqa E501
    # mock_response = mocker.Mock()
    # mock_response.status_code = 200
    # mock_response.json.return_value = {
    #     "geometry": lpa_geometry,
    # }
    #
    # # Mock the `requests.Session.get` method
    # mocker.patch("requests.get", return_value=mock_response)
    # set up cli runner

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--dataset",
            "conservation-area",
            "--pipeline-dir",
            str("tests/data/conservation-area/pipeline"),
            "dataset-create",
            "--output-path",
            f"{str(cache_path)}/conservation-area.sqlite3",
            "--organisation-path",
            str(organisation_path),
            "--column-field-dir",
            str("tests/data/conservation-area/var/column-field"),
            "--dataset-resource-dir",
            str("tests/data/conservation-area/var/dataset-resource"),
            "--issue-dir",
            str(cache_path),
            "--cache-dir",
            str(cache_path),
            str(input_paths[0]),
        ],
        catch_exceptions=False,
    )

    print("result.exit_code")
    print(result.exit_code)
    print("\n")
    # Check that the command exits with status code 0 (success)
    if result.exit_code != 0:
        # Print the command output if the test fails
        print("Command failed with exit code:", result.exit_code)
        print("Command output:")
        print(result.output)
        print("Command error output:")
        print(result.exception)

    assert result.exit_code == 0, "error returned when running expectations"

    print("cache_path")
    print(cache_path)
    output_path = f"{str(cache_path)}/conservation-area"
    pq_files = [file for file in os.listdir(output_path) if file.endswith(".parquet")]
    assert len(pq_files) == 3, "Not all parquet files created"
    assert np.all(
        np.sort(pq_files) == ["entity.parquet", "fact.parquet", "fact_resource.parquet"]
    ), "parquet file names not correct"
    sqlite_db_path = f"{output_path}/conservation-area.sqlite3"
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    tables = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table';"
    ).fetchall()
    expected_tables = {"fact", "fact_resource", "entity"}
    actual_tables = {table[0] for table in tables}
    missing_tables = expected_tables - actual_tables
    assert (
        len(missing_tables) == 0
    ), f"Missing following tables in sqlite database: {missing_tables}"

    for table in list(expected_tables):
        pq_rows = len(pd.read_parquet(f"{output_path}/{table}.parquet"))
        sql_rows = cursor.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        assert (
            pq_rows == sql_rows
        ), f"Different rows between the parquet files and database table for {table}"
