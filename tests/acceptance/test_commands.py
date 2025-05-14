import csv
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import json

import pandas as pd
import pytest
from digital_land.commands import dataset_update
from digital_land.pipeline.main import Pipeline
from digital_land.specification import Specification

dataset = "central-activities-zone"


def write_csv_file(path, headers, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def column_field_file(test_dirs):
    column_field_path = test_dirs["column_field_dir"] / dataset / "resource-hash.csv"
    column_field_headers = ["dataset", "resource", "column", "field"]
    column_field_data = [
        {
            "dataset": "central-activities-zone",
            "resource": "resource-hash",
            "column": "new_column",
            "field": "name",
        }
    ]
    write_csv_file(column_field_path, column_field_headers, column_field_data)
    return column_field_path


@pytest.fixture
def dataset_resource_file(test_dirs):
    dataset_resource_path = (
        test_dirs["dataset_resource_dir"] / dataset / "resource-hash.csv"
    )
    dataset_resource_headers = [
        "dataset",
        "resource",
        "entry-count",
        "line-count",
        "mime-type",
        "internal-path",
        "internal-mime-type",
    ]
    dataset_resource_data = [
        {
            "dataset": "central-activities-zone",
            "resource": "resource-hash",
            "entry-count": 1,
            "line-count": 1,
            "mime-type": "",
            "internal-path": "",
            "internal-mime-type": "",
        }
    ]
    write_csv_file(
        dataset_resource_path, dataset_resource_headers, dataset_resource_data
    )
    return dataset_resource_path


@pytest.fixture
def issue_file(test_dirs):
    issue_path = test_dirs["issues_log_dir"] / dataset / "resource-hash.csv"
    issue_headers = [
        "dataset,resource,line-number,entry-number,field,entity,issue-type,value,message"
    ]
    issue_data = []
    write_csv_file(issue_path, issue_headers, issue_data)
    return issue_path


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


# Testing dataset update ability to add new entity
def test_dataset_update_local_new_entity(
    test_dirs, column_field_file, dataset_resource_file, issue_file, organisation_path
):
    # existing test db path
    original_dataset_path = Path(f"tests/data/dataset/{dataset}.sqlite3")
    updated_dataset_path = tempfile.NamedTemporaryFile(suffix=".sqlite3").name

    # copy so we can update a version to compare to original
    shutil.copy(original_dataset_path, updated_dataset_path)

    # Create transformed file
    transformed_path = test_dirs["transformed_dir"] / dataset / "resource-hash.csv"
    transformed_headers = [
        "end-date",
        "entity",
        "entry-date",
        "entry-number",
        "fact",
        "field",
        "priority",
        "reference-entity",
        "resource",
        "start-date",
        "value",
    ]
    transformed_data = [
        {
            "end-date": "",
            "entity": 2200011,
            "entry-date": "9999-01-01",
            "entry-number": 1,
            "fact": "fact-hash",
            "field": "name",
            "priority": 2,
            "reference-entity": "",
            "resource": "resource-hash",
            "start-date": "",
            "value": "new name",
        }
    ]
    write_csv_file(transformed_path, transformed_headers, transformed_data)

    # Now run dataset_update function
    pipeline = Pipeline(test_dirs["pipeline_dir"], dataset)
    specification = Specification(test_dirs["specification_dir"])
    dataset_update(
        [transformed_path],
        "",
        organisation_path,
        pipeline,
        dataset,
        specification,
        issue_dir=test_dirs["issues_log_dir"],
        column_field_dir=test_dirs["column_field_dir"],
        dataset_resource_dir=test_dirs["dataset_resource_dir"],
        dataset_path=updated_dataset_path,
    )

    # Check rows have updated with new facts
    with sqlite3.connect(updated_dataset_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM entity where entity = '2200011';")
        entity_row = cursor.fetchone()
        assert entity_row["name"] == "new name"

        cursor.execute("SELECT * FROM column_field where resource = 'resource-hash';")
        column_field_row = cursor.fetchone()
        assert column_field_row["column"] == "new_column"

        cursor.execute(
            "SELECT * from dataset_resource where resource = 'resource-hash';"
        )
        dataset_resource_row = cursor.fetchone()
        assert dataset_resource_row["entity_count"] == 1

        cursor.execute("SELECT * from fact where fact = 'fact-hash';")
        fact_row = cursor.fetchone()
        assert fact_row["value"] == "new name"
        assert fact_row["entry_date"] == "9999-01-01"

        cursor.execute("SELECT * from fact_resource where fact = 'fact-hash';")
        fact_resource_row = cursor.fetchone()
        assert fact_resource_row["resource"] == "resource-hash"


# Testing dataset update function to update existing entity values
def test_dataset_update_local_existing_entity(
    test_dirs, column_field_file, dataset_resource_file, issue_file, organisation_path
):
    # existing test db path
    original_dataset_path = Path(f"tests/data/dataset/{dataset}.sqlite3")
    updated_dataset_path = tempfile.NamedTemporaryFile(suffix=".sqlite3").name

    # copy so we can update a version to compare to original
    shutil.copy(original_dataset_path, updated_dataset_path)

    # Create transformed file
    transformed_path = test_dirs["transformed_dir"] / dataset / "resource-hash.csv"
    transformed_headers = [
        "end-date",
        "entity",
        "entry-date",
        "entry-number",
        "fact",
        "field",
        "priority",
        "reference-entity",
        "resource",
        "start-date",
        "value",
    ]
    transformed_data = [
        {
            "end-date": "",
            "entity": 2200001,
            "entry-date": "9999-01-01",
            "entry-number": 1,
            "fact": "fact-hash",
            "field": "geometry",
            "priority": 2,
            "reference-entity": "",
            "resource": "resource-hash",
            "start-date": "",
            "value": "MULTIPOLYGON(((1 1, 1 2, 2 2, 2 1, 1 1)), ((0 0, 0 1, 1 1, 1 0, 0 0)))",
        },
        {
            "end-date": "",
            "entity": 2200001,
            "entry-date": "9999-01-01",
            "entry-number": 1,
            "fact": "fact-hash2",
            "field": "description",
            "priority": 2,
            "reference-entity": "",
            "resource": "resource-hash",
            "start-date": "",
            "value": "interesting description",
        },
    ]
    write_csv_file(transformed_path, transformed_headers, transformed_data)

    # Now run dataset_update function
    pipeline = Pipeline(test_dirs["pipeline_dir"], dataset)
    specification = Specification(test_dirs["specification_dir"])
    dataset_update(
        [transformed_path],
        "",
        organisation_path,
        pipeline,
        dataset,
        specification,
        issue_dir=test_dirs["issues_log_dir"],
        column_field_dir=test_dirs["column_field_dir"],
        dataset_resource_dir=test_dirs["dataset_resource_dir"],
        dataset_path=updated_dataset_path,
    )

    # Test changes
    with sqlite3.connect(updated_dataset_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM entity;")
        entity_rows = cursor.fetchall()
        assert len(entity_rows) == 10

        entity_row = entity_rows[0]
        # Check relevant fields have been updated
        assert (
            entity_row["geometry"]
            == "MULTIPOLYGON(((1 1, 1 2, 2 2, 2 1, 1 1)), ((0 0, 0 1, 1 1, 1 0, 0 0)))"
        )
        entity_json = json.loads(entity_row["json"])
        assert entity_json["description"] == "interesting description"

        # Now check that original data still exists
        assert "Central London Area - part of" in entity_json["notes"]
        assert entity_row["entry_date"] == "2024-10-17"
        assert entity_row["reference"] == "CAZ00000001"
