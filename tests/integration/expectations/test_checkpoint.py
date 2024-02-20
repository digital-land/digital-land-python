import pytest
import os
import spatialite
import yaml
import pandas as pd
from csv import DictReader
from digital_land.expectations.checkpoints.dataset import DatasetCheckpoint


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


def test_run_checkpoint_success(tmp_path, sqlite3_with_entity_tables_path):
    # load data
    test_entity_data = pd.DataFrame.from_dict(
        {"entity": [1], "name": ["test1"]}
    )
    test_old_entity_data = pd.DataFrame.from_dict(
        {"old_entity": [100], "entity": [10]}
    )
    with spatialite.connect(sqlite3_with_entity_tables_path) as con:
        test_entity_data.to_sql("entity", con, if_exists="append", index=False)
        test_old_entity_data.to_sql("old_entity", con, if_exists="append", index=False)

    checkpoint = DatasetCheckpoint("test-checkpoint", sqlite3_with_entity_tables_path, 'test', None)
    checkpoint.load()
    checkpoint.run()
    checkpoint.save(tmp_path)

    with open(os.path.join(tmp_path, "test-checkpoint", "test-responses.csv"), "r") as f:
        responses = list(DictReader(f))

    with open(os.path.join(tmp_path, "test-checkpoint", "test-issues.csv"), "r") as f:
        issues = list(DictReader(f))

    assert len(responses) == 1
    assert responses[0]['checkpoint'] == "test-checkpoint"
    assert responses[0]['result'] == "True"
    assert responses[0]['severity'] == "warning"
    assert responses[0]['msg'] == "No enities found in old-entities"

    assert len(issues) == 0

def test_run_checkpoint_failure(tmp_path, sqlite3_with_entity_tables_path):
    # load data
    test_entity_data = pd.DataFrame.from_dict(
        {"entity": [1], "name": ["test1"], "dataset": ["test-dataset"], "organisation_entity":["123"]}
    )
    test_old_entity_data = pd.DataFrame.from_dict(
        {"old_entity": [1], "entity": [10]}
    )
    with spatialite.connect(sqlite3_with_entity_tables_path) as con:
        test_entity_data.to_sql("entity", con, if_exists="append", index=False)
        test_old_entity_data.to_sql("old_entity", con, if_exists="append", index=False)

    checkpoint = DatasetCheckpoint("test-checkpoint", sqlite3_with_entity_tables_path, 'test', None)
    checkpoint.load()
    checkpoint.run()
    checkpoint.save(tmp_path)

    with open(os.path.join(tmp_path, "test-checkpoint", "test-responses.csv"), "r") as f:
        responses = list(DictReader(f))

    with open(os.path.join(tmp_path, "test-checkpoint", "test-issues.csv"), "r") as f:
        issues = list(DictReader(f))

    assert len(responses) == 1
    assert responses[0]['checkpoint'] == "test-checkpoint"
    assert responses[0]['result'] == "False"
    assert responses[0]['severity'] == "warning"
    assert responses[0]['msg'] == "1 enities found in old-entities"

    assert len(issues) == 1
    assert responses[0]['checkpoint'] == "test-checkpoint"
    assert responses[0]['result'] == "False"
    assert responses[0]['severity'] == "warning"
