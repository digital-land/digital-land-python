"""
A set of tests to mimic a user (computational or otherwise) running tests against
a sqlite dataset. There are quite a few things to set up and this specifically
"""

import pytest
import csv
import numpy as np
import pandas as pd
import os
import sqlite3
from tempfile import TemporaryDirectory
from pathlib import Path

from click.testing import CliRunner

from digital_land.cli import cli
from digital_land.configuration.main import Config
from digital_land.specification import Specification

testing_dir = "conservation-area"


@pytest.fixture(scope="session")
def session_tmp_path():
    with TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def input_paths():
    input_paths = []
    directory = f"tests/data/{testing_dir}/transformed/{testing_dir}/"
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
    orgs_path = f"tests/data/{testing_dir}/organisation.csv"

    return orgs_path


@pytest.fixture
def config_path(session_tmp_path, specification_dir):
    """create a configuration to use"""
    config_path = session_tmp_path / "config.sqlite3"
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
    expect_path = session_tmp_path / "expect.csv"
    # Writing data to CSV
    with open(expect_path, mode="w", newline="") as file:
        # Define the fieldnames (column headers) based on dictionary keys
        writer = csv.DictWriter(file, fieldnames=rules[0].keys())
        writer.writeheader()  # Write the header row
        writer.writerows(rules)  # Write each dictionary as a row

    spec = Specification(specification_dir)
    config = Config(path=config_path, specification=spec)
    config.create()
    config.load({"expect": str(session_tmp_path)})
    return config_path


@pytest.fixture
def cache_path(session_tmp_path):
    cache_path = session_tmp_path / "var/cache"
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
    return cache_path


@pytest.fixture
def output_dir(session_tmp_path):
    output_dir = Path(f"dataset/{testing_dir}")
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def test_run_some_expectations(
    session_tmp_path,
    organisation_path,
    input_paths,
    config_path,
    specification_dir,
    cache_path,
    output_dir,
):
    output_path = output_dir / f"{testing_dir}.sqlite3"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--dataset",
            str(testing_dir),
            "--pipeline-dir",
            str(f"tests/data/{testing_dir}/pipeline"),
            "dataset-create",
            "--output-path",
            str(output_path),
            "--organisation-path",
            str(organisation_path),
            "--column-field-dir",
            str(f"tests/data/{testing_dir}/var/column-field"),
            "--dataset-resource-dir",
            str(f"tests/data/{testing_dir}/var/dataset-resource"),
            "--issue-dir",
            str(cache_path),
            "--cache-dir",
            str(cache_path),
            str(input_paths[0]),
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

    assert result.exit_code == 0, "error returned when running expectations"
    pq_files = [file for file in os.listdir(cache_path) if file.endswith(".parquet")]
    assert len(pq_files) == 3, "Not all parquet files created"
    assert np.all(
        np.sort(pq_files) == ["entity.parquet", "fact.parquet", "fact_resource.parquet"]
    ), "parquet file names not correct"

    assert os.path.exists(
        output_path
    ), f"sqlite file {testing_dir}.sqlite3 does not exists"
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
        pq_rows = len(pd.read_parquet(f"{cache_path}/{table}.parquet"))
        sql_rows = cursor.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        assert (
            pq_rows == sql_rows
        ), f"Different rows between the parquet files and database table for {table}"
