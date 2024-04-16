import pytest
import os
import spatialite
import pandas as pd
from csv import DictReader, DictWriter
from digital_land.expectations.checkpoints.dataset import DatasetCheckpoint
from digital_land.expectations.expectation_functions.resource_validations import (
    check_for_duplicate_references,
    validate_references,
)


@pytest.fixture
def sqlite3_with_entity_tables_path(tmp_path):
    dataset_path = os.path.join(tmp_path, "test.sqlite3")

    create_entity_table_sql = """
        CREATE TABLE entity (
            dataset TEXT,
            end_date TEXT,
            entity INTEGER PRIMARY KEY,
            entry_date TEXT,
            geojson JSON,
            geometry TEXT,
            json JSON,
            name TEXT,
            organisation_entity TEXT,
            point TEXT,
            prefix TEXT,
            reference TEXT,
            start_date TEXT,
            typology TEXT
        );
    """

    create_old_entity_table_sql = """
        CREATE TABLE old_entity (
            old_entity INTEGER PRIMARY KEY,
            entity INTEGER
        );
    """

    with spatialite.connect(dataset_path) as con:
        con.execute(create_entity_table_sql)
        con.execute(create_old_entity_table_sql)

    return dataset_path


@pytest.fixture
def csv_path(tmp_path):
    data = [
        {"reference": "REF-001", "name": "Test 1"},
        {"reference": "REF-002", "name": "Test 2"},
        {"reference": "REF-001", "name": "Test 3"},  # Duplicate
        {"reference": "", "name": "Test 4"},  # Invalid format
    ]
    csv_file = tmp_path / "test_data.csv"
    with csv_file.open(mode="w", newline="") as f:
        writer = DictWriter(f, fieldnames=["reference", "name"])
        writer.writeheader()
        writer.writerows(data)
    return csv_file


def test_run_checkpoint_success(tmp_path, sqlite3_with_entity_tables_path):
    # load data
    test_entity_data = pd.DataFrame.from_dict({"entity": [1], "name": ["test1"]})
    test_old_entity_data = pd.DataFrame.from_dict({"old_entity": [100], "entity": [10]})
    with spatialite.connect(sqlite3_with_entity_tables_path) as con:
        test_entity_data.to_sql("entity", con, if_exists="append", index=False)
        test_old_entity_data.to_sql("old_entity", con, if_exists="append", index=False)

    checkpoint = DatasetCheckpoint(
        sqlite3_with_entity_tables_path,
        None,
    )
    checkpoint.load()
    checkpoint.run()
    checkpoint.save(tmp_path)

    with open(os.path.join(tmp_path, "dataset", "test-responses.csv"), "r") as f:
        responses = list(DictReader(f))

    with open(os.path.join(tmp_path, "dataset", "test-issues.csv"), "r") as f:
        issues = list(DictReader(f))

    assert len(responses) == 1
    assert responses[0]["checkpoint"] == "dataset"
    assert responses[0]["passed"] == "True"
    assert responses[0]["severity"] == "warning"
    assert responses[0]["message"] == "No retired enities found in the dataset."

    assert len(issues) == 0


def test_run_checkpoint_failure(tmp_path, sqlite3_with_entity_tables_path):
    # load data
    test_entity_data = pd.DataFrame.from_dict(
        {
            "entity": [1],
            "name": ["test1"],
            "dataset": ["test-dataset"],
            "organisation_entity": ["123"],
        }
    )
    test_old_entity_data = pd.DataFrame.from_dict({"old_entity": [1], "entity": [10]})
    with spatialite.connect(sqlite3_with_entity_tables_path) as con:
        test_entity_data.to_sql("entity", con, if_exists="append", index=False)
        test_old_entity_data.to_sql("old_entity", con, if_exists="append", index=False)

    checkpoint = DatasetCheckpoint(
        sqlite3_with_entity_tables_path,
        None,
    )
    checkpoint.load()
    checkpoint.run()
    checkpoint.save(tmp_path)

    with open(os.path.join(tmp_path, "dataset", "test-responses.csv"), "r") as f:
        results = list(DictReader(f))

    with open(os.path.join(tmp_path, "dataset", "test-issues.csv"), "r") as f:
        issues = list(DictReader(f))

    assert len(results) == 1
    assert (
        results[0]["expectation-result"] != ""
    )  # Don't care what it is, as long as it's there
    assert results[0]["passed"] == "False"
    assert (
        results[0]["message"]
        == "There are 1 enities which have been retired but are still present in the dataset."
    )
    assert results[0]["severity"] == "warning"
    assert results[0]["checkpoint"] == "dataset"
    assert results[0]["data-name"] == "test"

    assert len(issues) == 1
    assert (
        issues[0]["expectation-result"] == results[0]["expectation-result"]
    )  # Should match the response
    assert issues[0]["scope"] == "row"
    assert (
        issues[0]["message"]
        == "Entity 1 has been retired but is still in the dataset. This needs investigating."
    )
    assert issues[0]["dataset"] == "test-dataset"
    assert issues[0]["organisation"] == "123"
    assert issues[0]["table-name"] == "entity"
    assert issues[0]["row-id"] == "1"
    assert issues[0]["rows"] == ""
    assert issues[0]["row"] != ""  # Just check it's there
    assert issues[0]["value"] == ""


def test_check_for_duplicate_references(csv_path):
    _, _, issues = check_for_duplicate_references(csv_path)

    assert issues, "The function should successfully identify issues."
    assert len(issues) == 1, "There should be one issue identified."
    assert (
        issues[0]["scope"] == "row-group"
    ), "The issue should be identified as a duplicate reference."
    assert (
        "REF-001" in issues[0]["message"]
    ), "REF-001 should be identified as a duplicate."


def test_validate_references(csv_path):
    _, _, issues = validate_references(csv_path)

    assert issues, "The function should fail due to invalid references."
    assert len(issues) == 1, "There should be one issue identified."
    assert (
        issues[0]["scope"] == "value"
    ), "The issue should be identified as an invalid reference."
    assert "" in issues[0]["message"], " 4th value should be identified as invalid."
