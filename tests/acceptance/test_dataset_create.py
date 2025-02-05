"""
A set of tests to mimic a user (computational or otherwise) running tests against
a sqlite dataset. There are quite a few things to set up and this specifically
"""

import pytest

import numpy as np
import pandas as pd
import os
import sqlite3
from tempfile import TemporaryDirectory
from pathlib import Path

from click.testing import CliRunner

from digital_land.cli import cli

test_collection = "conservation-area"
test_dataset = "conservation-area"


@pytest.fixture(scope="session")
def session_tmp_path():
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def input_paths():
    input_paths = []
    directory = f"tests/data/{test_collection}/transformed/{test_dataset}/"
    for root, dirs, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(root, file)
            input_paths.append(full_path)

    return input_paths


@pytest.fixture
def organisation_path():
    """
    build an organisations dataset to use
    """
    orgs_path = f"tests/data/{test_collection}/organisation.csv"
    return orgs_path


@pytest.fixture
def cache_path(session_tmp_path):
    cache_path = session_tmp_path / "var" / "cache"
    os.makedirs(cache_path, exist_ok=True)
    return cache_path


@pytest.fixture
def dataset_dir(session_tmp_path):
    dataset_dir = session_tmp_path / "dataset"
    os.makedirs(dataset_dir, exist_ok=True)
    return dataset_dir


@pytest.fixture
def issue_dir(session_tmp_path):
    issue_dir = session_tmp_path / "issue"
    os.makedirs(issue_dir, exist_ok=True)
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
    input_paths,
    issue_dir,
    cache_path,
    dataset_dir,
    resource_path,
):
    output_path = dataset_dir / f"{test_dataset}.sqlite3"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--dataset",
            str(test_dataset),
            "--pipeline-dir",
            str(f"tests/data/{test_collection}/pipeline"),
            "dataset-create",
            "--output-path",
            str(output_path),
            "--organisation-path",
            str(organisation_path),
            "--column-field-dir",
            str(f"tests/data/{test_collection}/var/column-field"),
            "--dataset-resource-dir",
            str(f"tests/data/{test_collection}/var/dataset-resource"),
            "--issue-dir",
            str(issue_dir),
            "--cache-dir",
            str(cache_path),
            "--resource-path",
            str(resource_path),
        ]
        + input_paths,
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

    assert result.exit_code == 0, "error returned when building dataset"
    pq_cache = os.path.join(cache_path, test_dataset)
    pq_files = [file for file in os.listdir(pq_cache) if file.endswith(".parquet")]
    assert len(pq_files) == 3, "Not all parquet files created"
    assert np.all(
        np.sort(pq_files) == ["entity.parquet", "fact.parquet", "fact_resource.parquet"]
    ), "parquet file names not correct"

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

    for table in list(expected_tables):
        pq_rows = len(pd.read_parquet(f"{pq_cache}/{table}.parquet"))
        sql_rows = cursor.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        assert (
            pq_rows == sql_rows
        ), f"Different rows between the parquet files and database table for {table}"
