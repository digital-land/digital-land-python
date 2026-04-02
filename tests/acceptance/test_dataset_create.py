"""
A set of tests to mimic a user (computational or otherwise) running tests against
a sqlite dataset. There are quite a few things to set up and this specifically
"""

import pytest
import logging
import numpy as np
import pandas as pd
import os
import sqlite3
from tempfile import TemporaryDirectory
from pathlib import Path

from click.testing import CliRunner

from digital_land.cli import cli

TEST_COLLECTION = "conservation-area"
TEST_DATASET = "conservation-area"


@pytest.fixture(scope="session")
def session_tmp_path():
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def cache_path(tmp_path):
    cache_path = tmp_path / "var" / "cache"
    cache_path.mkdir(parents=True, exist_ok=True)
    return cache_path


test_geometry = "MULTIPOLYGON(((-0.49901924 53.81622,-0.5177418 53.76114,-0.4268378 53.78454,-0.49901924 53.81622)))"
transformed_1_data = {
    "end_date": [np.nan] * 16,
    "entity": [11, 11, 11, 11, 11, 11, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12],
    "entry_date": [
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-01-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
        "2023-02-01",
    ],
    "entry_number": [2] * 16,
    "fact": [
        "abcdef1",
        "abcdef2",
        "abcdef3",
        "abcdef4",
        "abcdef5",
        "abcdef6",
        "abc1231",
        "abc1232",
        "abc1233",
        "def4561",
        "def4562",
        "def4563",
        "a1b2c31",
        "a1b2c32",
        "a1b2c33",
        "a1b2c34",
    ],
    "field": [
        "entry-date",
        "geometry",
        "point",
        "document-url",
        "organisation",
        "entry-date",
        "geometry",
        "organisation",
        "entry-date",
        "geometry",
        "organisation",
        "entry-date",
        "geomtry",
        "document-url",
        "notes-checking",
        "organisation",
    ],
    "priority": [2] * 16,
    "reference_entity": [np.nan] * 16,
    "resource": [
        "zyxwvu",
        "zyxwvu",
        "zyxwvu",
        "zyxwvu",
        "zyxwvu",
        "zyxwvu",
        "yxwvut",
        "yxwvut",
        "zyxwvu",
        "xwvuts",
        "xwvuts",
        "zyxwvu",
        "wvutsr",
        "wvutsr",
        "wvutsr",
        "wvutsr",
    ],
    "start_date": [np.nan] * 16,
    "value": [
        "2023-01-01",
        f"{test_geometry}",
        '"POINT(-0.481 53.788)"',
        "https://www.test.xyz",
        "organisation:AAA",
        "2023-01-01",
        f"{test_geometry}",
        "local-authority:BBB",
        "2023-01-01",
        f"{test_geometry}",
        "local-authority:CCC",
        "2023-01-01",
        f"{test_geometry}",
        "https://www.testing.yyz",
        "Something random",
        "local-authority:DDD",
    ],
}


@pytest.fixture
def input_dir(cache_path):
    data_dicts = {"resource_1": transformed_1_data}
    input_paths = []
    directory = cache_path / "transformed_parquet" / "conservation-area"
    directory.mkdir(parents=True, exist_ok=True)

    for path, data in data_dicts.items():
        data = pd.DataFrame.from_dict(data)
        input_path = directory / f"{path}.parquet"
        data.to_parquet(input_path, index=False)
        logging.error(str(input_path))
        input_paths.append(str(input_path))

    return directory


@pytest.fixture
def organisation_path(tmp_path):
    """
    build an organisations dataset to use
    """
    org_data = {
        "entity": [101, 102],
        "name": ["test", "test_2"],
        "prefix": ["local-authority", "local-authority"],
        "reference": ["test", "test_2"],
        "dataset": ["local-authority", "local-authority"],
        "organisation": ["local-authority:test", "local-authority:test_2"],
    }
    orgs_path = tmp_path / "organisation.csv"

    pd.DataFrame.from_dict(org_data).to_csv(orgs_path, index=False)
    return orgs_path


@pytest.fixture
def column_field_path(tmp_path):
    column_field_dir = tmp_path / "column-field"
    dataset_cfd = column_field_dir / "conservation-area"
    (dataset_cfd).mkdir(parents=True, exist_ok=True)
    data = {
        "end_date": [""],
        "entry_date": [""],
        "field": ["geometry"],
        "dataset": ["conservation-area"],
        "start_date": [""],
        "resource": [""],
        "column": ["WKT"],
    }
    pd.DataFrame.from_dict(data).to_csv(dataset_cfd / "resource_1.csv", index=False)
    logging.error(str(dataset_cfd / "resource_1.csv"))
    return column_field_dir


@pytest.fixture
def dataset_resource_path(tmp_path):
    dataset_resource_path = tmp_path / "dataset-resource"
    dataset_drd = dataset_resource_path / "conservation-area"
    dataset_drd.mkdir(parents=True, exist_ok=True)
    data = {
        "end_date": [""],
        "entry_date": [""],
        "dataset": ["conservation-area"],
        "entity_count": [""],
        "entry_count": [1],
        "line_count": [1],
        "mime_type": [""],
        "internal_path": [""],
        "internal_mime_type": [""],
        "resource": ["resource_1"],
        "start_date": [""],
    }
    pd.DataFrame.from_dict(data).to_csv(dataset_drd / "resource_1.csv", index=False)
    return dataset_resource_path


@pytest.fixture
def dataset_dir(session_tmp_path):
    dataset_dir = session_tmp_path / "dataset"
    os.makedirs(dataset_dir, exist_ok=True)
    return dataset_dir


@pytest.fixture
def issue_dir(session_tmp_path):
    issue_dir = session_tmp_path / "issue"
    os.makedirs(issue_dir, exist_ok=True)

    # Create test issue files for each dataset
    dataset = TEST_DATASET
    dataset_issue_dir = issue_dir / dataset
    os.makedirs(dataset_issue_dir, exist_ok=True)

    # Create a sample issue CSV file
    issue_file = dataset_issue_dir / "test-resource.csv"
    issue_data = {
        "dataset": [dataset, dataset, dataset],
        "resource": ["test-resource", "test-resource", "test-resource"],
        "line-number": [2, 3, 4],
        "entry-number": [1, 2, 3],
        "field": ["name", "reference", "start-date"],
        "entity": ["", "12345", ""],
        "issue-type": ["missing value", "invalid format", "invalid date"],
        "value": ["", "INVALID-REF", "2023-13-45"],
        "message": [
            "name field is required",
            "reference format is invalid",
            "date must be in format YYYY-MM-DD",
        ],
    }
    df = pd.DataFrame(issue_data)
    df.to_csv(issue_file, index=False)

    return issue_dir


@pytest.fixture
def resource_path(session_tmp_path):
    resource_path = session_tmp_path / "resource.csv"
    columns = ["resource", "end-date"]
    with open(resource_path, "w") as f:
        f.write(",".join(columns) + "\n")
    return resource_path


def test_acceptance_dataset_create(
    session_tmp_path,
    organisation_path,
    input_dir,
    issue_dir,
    cache_path,
    dataset_dir,
    resource_path,
    column_field_path,
    dataset_resource_path,
):
    output_path = dataset_dir / f"{TEST_DATASET}.sqlite3"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--dataset",
            str(TEST_DATASET),
            "--pipeline-dir",
            str(f"tests/data/{TEST_COLLECTION}/pipeline"),
            "dataset-create",
            "--output-path",
            str(output_path),
            "--organisation-path",
            str(organisation_path),
            "--column-field-dir",
            str(column_field_path),
            "--dataset-resource-dir",
            str(dataset_resource_path),
            "--issue-dir",
            str(issue_dir),
            "--cache-dir",
            str(cache_path),
            "--resource-path",
            str(resource_path),
            str(input_dir),
        ],
        catch_exceptions=False,
    )

    # Check that the command exits with status code 0 (success)
    if result.exit_code != 0:
        # Print the command output if the test fails
        print("Command failed with exit code:", result.exit_code)
        print("Command output:")
        print(result.output)
        print("Command error output:")
        print(result.exception)

    # get filepath for each parquet file

    entity_parquet_path = (
        cache_path
        / "provenance"
        / "entity"
        / "dataset=conservation-area"
        / "entity.parquet"
    )
    fact_parquet_path = (
        cache_path
        / "provenance"
        / "fact"
        / "dataset=conservation-area"
        / "fact.parquet"
    )
    fact_resource_parquet_path = (
        cache_path
        / "provenance"
        / "fact-resource"
        / "dataset=conservation-area"
        / "fact-resource.parquet"
    )
    files = [
        entity_parquet_path,
        fact_parquet_path,
        fact_resource_parquet_path,
    ]
    for file in files:
        assert file.exists(), f"file {file.name} not created."
    assert result.exit_code == 0, "error returned when building dataset"

    # check that parquet files have been created correctlly in the cache directory
    # may  want to adjust this for how we structure  a parquet package in the future
    # also we are using the cache to store this for now but in the future  we may  want to store it in a specific directory

    # Check the sqlite file was created
    assert os.path.exists(output_path), f"sqlite file {output_path} does not exists"

    conn = sqlite3.connect(output_path)
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

    for file in files:

        pq_rows = len(pd.read_parquet(file))

        assert pq_rows > 0, f"parquet file {file.stem} is empty"
        sql_rows = cursor.execute(
            f"SELECT COUNT(*) FROM {file.stem.replace('-', '_')};"
        ).fetchone()[0]
        assert sql_rows > 0, f"database table {file.stem} is empty"
        assert (
            pq_rows == sql_rows
        ), f"Different rows between the parquet files and database table for {file.stem}"

    # entity table specific tests to check how we expect the data to be used

    # json field checks
    # where no json  value  is present  we  expect the value to be null. not blank or an empty json bracket
    # so will ensure these aren't in the results of  any test
    sql = """
        SELECT *
        FROM entity
        WHERE json = '{}'
        ;"""

    results = cursor.execute(sql).fetchall()
    assert (
        len(results) == 0
    ), "there should be no rows where json is an empty json bracket"

    # check no json values are arrays
    sql = """
        SELECT *
        FROM entity
        WHERE json_type(json) NOT IN ('object', NULL)
        ;"""

    results = cursor.execute(sql).fetchall()
    assert len(results) == 0, "all json values should be objects or null"
