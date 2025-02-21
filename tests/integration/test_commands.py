import pytest
import csv
import os
import urllib.request
import pandas as pd
from pathlib import Path
import sqlite3
import tempfile
import shutil
import boto3
from moto import mock_aws
import logging

from digital_land.package.dataset import DatasetPackage
from digital_land.package.package import Specification
from digital_land.organisation import Organisation

""" dataset_create & dataset_update """


@pytest.fixture(scope="session")
def temp_dir():
    """Create a session-scoped temporary directory."""
    temp_directory = Path(tempfile.mkdtemp())  # Create a temp directory
    yield temp_directory
    shutil.rmtree(temp_directory)  # Cleanup after session


@pytest.fixture(scope="session")
def transformed_data_fixture():
    """Provides current and new resource data."""
    return {
        "current": [
            {
                "end-date": "",
                "entity": "44011910",
                "entry-date": "2024-10-24",
                "entry-number": "1",
                "fact": "baacaa9cb494d81e79b619ce314e0821713a2f2f7684a369633a543895206d76",
                "field": "document-url",
                "priority": "2",
                "reference-entity": "",
                "resource": "003286bd55d0d190e090901e789b2480eb39a94d5a116f0219d9a68cbb096f00",
                "start-date": "",
                "value": "https://www.southhams.gov.uk/sites/default/files/2023-07/Ashprington%20Conservation%20Area%20Appraisal.pdf",
            }
        ],
        "new": [
            {
                "end-date": "",
                "entity": "44011913",
                "entry-date": "2024-10-25",
                "entry-number": "50",
                "fact": "c59152d0c82d12ce925f91f62ec254aabfbc04f09928dba9c14d54c705568652",
                "field": "reference",
                "priority": "2",
                "reference-entity": "",
                "resource": "180bd32ec0b814e2b117fb76650fbc641ef6d1412fc8beae23a6287ff406464a",
                "start-date": "",
                "value": "211_MDE_CA50",
            }
        ],
    }


@pytest.fixture(scope="session")
def resource_files_fixture(transformed_data_fixture, temp_dir):
    """Creates resource CSV files and returns their paths."""
    dataset = "conservation-area"
    resources = [
        "1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3",
        "3bea83c3bea83cf3bea83cf3bea83cf3bea83cf3bea83cf13bea83cf3bea83cf",
    ]

    transformed_dir = temp_dir / "transformed"
    column_field_dir = temp_dir / "var/column-field" / dataset
    dataset_resource_dir = temp_dir / "var/dataset-resource" / dataset
    issue_dir = temp_dir / "issue" / dataset
    pipeline_dir = temp_dir / "pipeline"

    transformed_dir.mkdir(parents=True, exist_ok=True)
    column_field_dir.mkdir(parents=True, exist_ok=True)
    dataset_resource_dir.mkdir(parents=True, exist_ok=True)
    issue_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    csv_filename = pipeline_dir / "old-entity.csv"
    data = [
        ["old-entity", "status", "entity", "end-date", "notes", "entry-date", "start-date"],
        [44011900, 301, 44011910, "", "", "", ""]
    ]
    with open(csv_filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(data)
    csv_filename = pipeline_dir / "old-entity_updated.csv"
    data = [
        ["old-entity", "status", "entity", "end-date", "notes", "entry-date", "start-date"],
        [44011900, 301, 44011910, "", "", "", ""],
        [44011903, 301, 44011913, "", "", "", ""]
    ]
    with open(csv_filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(data)

    all_input_paths = []
    for i, res in enumerate(resources):
        csv_filename = transformed_dir / f"{res}.csv"
        with open(csv_filename, "w", newline="") as file:
            fieldnames = transformed_data_fixture["current"][
                0
            ].keys()  # Extract column names
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()  # Write column headers
            writer.writerows(
                transformed_data_fixture["current"]
                if i == 0
                else transformed_data_fixture["new"]
            )
        all_input_paths.append(csv_filename)

        # Create column field files
        csv_filename = column_field_dir / f"{res}.csv"
        data = [
            ["dataset", "resource", "column", "field"],
            ["conservation-area", res, "organisation", "IGNORE"],
            ["conservation-area", res, "reference", "reference"],
            ["conservation-area", res, "designation-date", "designation-date"],
            ["conservation-area", res, "document-url", "document-url"],
            ["conservation-area", res, "documentation-url", "documentation-url"],
            ["conservation-area", res, "end_date", "end-date"],
            ["conservation-area", res, "entry_date", "entry-date"],
            ["conservation-area", res, "geometry", "geometry"],
            ["conservation-area", res, "name", "name"],
            ["conservation-area", res, "notes", "notes"],
            ["conservation-area", res, "point", "point"],
            ["conservation-area", res, "start_date", "start-date"],
        ]
        with open(csv_filename, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(data)

        # Create dataset resource files
        csv_filename = dataset_resource_dir / f"{res}.csv"
        data = [
            [
                "dataset",
                "resource",
                "entry-count",
                "line-count",
                "mime-type",
                "internal-path",
                "internal-mime-type",
            ],
            ["conservation-area", res, 27, 28, "text/csv;charset=ASCII", "", ""],
        ]
        # ['end-date', 'entry-date', 'dataset', 'entity-count', 'entry-count', 'line-count', 'mime-type', 'internal-path',
        #  'internal-mime-type', 'resource', 'start-date']

        with open(csv_filename, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(data)

        # Create dataset resource files
        csv_filename = issue_dir / f"{res}.csv"
        line_number = 2 + (res != resources[0])
        entry_number = 1 + (res != resources[0])
        entity_no = 44011910 + 3*(res != resources[0])
        data = [
            [
                "dataset",
                "resource",
                "line-number",
                "entry-number",
                "field",
                "entity",
                "issue-type",
                "value",
                "message",
            ],
            [
                "conservation-area",
                res,
                line_number,
                entry_number,
                "document-url",
                entity_no,
                "invalid URI",
                "",
                "Test message",
            ],
        ]

        with open(csv_filename, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(data)

    return {
        "transformed_dir": transformed_dir,
        "column_field_dir": column_field_dir,
        "dataset_resource_dir": dataset_resource_dir,
        "issue_dir": issue_dir,
        "pipeline_dir": pipeline_dir,
        "all_input_paths": all_input_paths,
    }


@pytest.fixture(scope="session")
def specification_fixture_new_resources(temp_dir):
    # create custom specification to feed in
    schema = {
        "old-entity": {
            "fields": [
                "end-date",
                "entity",
                "entry-date",
                "notes",
                "old-entity",
                "start-date",
                "status",
            ]
        },
        "conservation-area": {"entity-minimum": "1", "entity-maximum": "2"},
        "entity": {"fields": []},
    }
    field = {
        "end-date": {
            "datatype": "datetime",
        },
        "entity": {
            "datatype": "integer",
        },
        "entry-date": {"datatype": "datetime"},
        "notes": {"datatype": "text"},
        "old-entity": {
            "datatype": "integer",
        },
        "start-date": {
            "datatype": "datetime",
        },
        "status": {
            "datatype": "string",
        },
    }

    specification = Specification(
        specification_dir=temp_dir, schema=schema, field=field
    )

    return specification


@pytest.fixture(scope="session")
def organisation_csv_new_resources(temp_dir):
    organisation_path = os.path.join(temp_dir, "organisation.csv")
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv",
        organisation_path,
    )
    return organisation_path


@pytest.fixture(scope="session")
def blank_patch_csv_new_resources(temp_dir):
    patch_path = os.path.join(temp_dir, "organisation.csv")
    fieldnames = ["dataset", "resource", "field", "pattern", "value"]
    with open(patch_path, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
    return patch_path


@pytest.fixture(scope="session")
def test_dataset_create_fixture(
    specification_dir,
    resource_files_fixture,
    organisation_csv_new_resources,
    blank_patch_csv_new_resources,
    temp_dir,
):
    """Creates and initializes the SQLite3 file with resources."""
    dataset = "conservation-area"
    sqlite3_path = temp_dir / f"{dataset}.sqlite3"

    organisation = Organisation(
        organisation_path=organisation_csv_new_resources,
        pipeline_dir=Path(os.path.dirname(blank_patch_csv_new_resources)),
    )
    package = DatasetPackage(
        dataset,
        organisation=organisation,
        path=sqlite3_path,
        specification_dir=specification_dir,  # TBD: package should use this specification object
    )

    # Set up initial SQLite3 file with tables
    package.create()
    path = resource_files_fixture["all_input_paths"][0]
    package.load_transformed(path)
    package.load_column_fields(
        resource_files_fixture["column_field_dir"] / Path(path).name
    )
    package.load_dataset_resource(
        resource_files_fixture["dataset_resource_dir"] / Path(path).name
    )
    package.load_entities()

    old_entity_path = os.path.join(resource_files_fixture["pipeline_dir"], "old-entity.csv")
    if os.path.exists(old_entity_path):
        package.load_old_entities(old_entity_path)

    issue_paths = resource_files_fixture["issue_dir"]
    if os.path.exists(issue_paths):
        for issue_path in os.listdir(issue_paths):
            package.load_issues(os.path.join(issue_paths, issue_path))
    else:
        logging.warning("No directory for this dataset in the provided issue_directory")

    package.add_counts()

    assert sqlite3_path.exists(), "SQLite file should exist after setup"

    # Fetch data for validation
    package.connect()
    package.create_cursor()
    package.cursor.execute("SELECT * FROM fact;")

    cols = [column[0] for column in package.cursor.description]
    actual_result = pd.DataFrame.from_records(
        package.cursor.fetchall(), columns=cols
    ).to_dict(orient="records")
    package.disconnect()

    # Copy file to new location and use this later on when checking to original
    orig_sqlite3_path = temp_dir / f"orig_{dataset}.sqlite3"
    shutil.copy(sqlite3_path, orig_sqlite3_path)

    return {
        "sqlite3_path": sqlite3_path,
        "orig_sqlite3_path": orig_sqlite3_path,
        "package": package,
        "actual_result": actual_result,
        "dataset": dataset,
    }


@pytest.fixture
def test_dataset_update_fixture(
    test_dataset_create_fixture,
    specification_dir,
    resource_files_fixture,
    organisation_csv_new_resources,
    blank_patch_csv_new_resources,
    temp_dir,
):
    """Creates and initializes the SQLite3 file with resources."""
    dataset = test_dataset_create_fixture["dataset"]
    sqlite3_path = test_dataset_create_fixture["sqlite3_path"]

    organisation = Organisation(
        organisation_path=organisation_csv_new_resources,
        pipeline_dir=Path(os.path.dirname(blank_patch_csv_new_resources)),
    )
    package = DatasetPackage(
        dataset,
        organisation=organisation,
        path=sqlite3_path,
        specification_dir=specification_dir,  # TBD: package should use this specification object
    )

    bucket_name = "test-collection-data"
    object_key = f"test-collection/{dataset}"
    table_name = dataset

    # Set up initial SQLite3 file with tables
    package.load_from_s3(bucket_name, object_key, table_name)
    package.connect()
    package.create_cursor()
    path = resource_files_fixture["all_input_paths"][1]
    package.load_transformed(path)
    package.load_column_fields(
        resource_files_fixture["column_field_dir"] / Path(path).name
    )
    package.load_dataset_resource(
        resource_files_fixture["dataset_resource_dir"] / Path(path).name
    )
    package.load_entities()

    old_entity_path = os.path.join(resource_files_fixture["pipeline_dir"], "old-entity_updated.csv")
    if os.path.exists(old_entity_path):
        package.load_old_entities(old_entity_path)

    issue_paths = resource_files_fixture["issue_dir"]
    if os.path.exists(issue_paths):
        for issue_path in os.listdir(issue_paths):
            package.load_issues(os.path.join(issue_paths, issue_path))
    else:
        logging.warning("No directory for this dataset in the provided issue_directory")

    assert sqlite3_path.exists(), "SQLite file should exist after setup"

    # Fetch data for validation
    package.connect()
    package.create_cursor()
    package.cursor.execute("SELECT * FROM fact;")

    package.disconnect()

    return {
        "package": package,
    }


def get_table_row_counts(cursor):
    """Returns a dictionary with table names as keys and their row counts as values."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    row_counts = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        row_counts[table] = cursor.fetchone()[0]

    return row_counts


def test_create_tables_fixture(test_dataset_create_fixture):
    """Need to set up an initial sqlite3 file before sending it to S3 and then jupdating it."""
    package = test_dataset_create_fixture["package"]
    sqlite3_path = test_dataset_create_fixture["sqlite3_path"]

    assert sqlite3_path.exists(), "SQLite file should be present for the test"

    # Fetch and validate data
    package.connect()
    package.create_cursor()
    package.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = package.cursor.fetchall()
    orig_values = {}

    for table in tables:
        table_name = table[0]
        package.cursor.execute(f"SELECT * FROM {table_name};")
        cols = [column[0] for column in package.cursor.description]
        orig_values[table_name] = pd.DataFrame.from_records(
            package.cursor.fetchall(), columns=cols
        ).to_dict(orient="records")

    assert len(orig_values["fact"]) > 0, "Fact table should not be empty"
    assert len(orig_values["fact"]) == 1, "No. of facts not correct"
    assert (
        test_dataset_create_fixture["actual_result"] == orig_values["fact"]
    ), "results differ across scopes"


@pytest.fixture(scope="session")
def test_mock_s3_bucket(test_dataset_create_fixture):
    sqlite3_path = test_dataset_create_fixture["sqlite3_path"]
    dataset = test_dataset_create_fixture["dataset"]
    bucket_name = "test-collection-data"
    object_key = f"test-collection/{dataset}"

    """Sets up a mock S3 bucket using moto."""
    with mock_aws():
        # Current AWS region we use is 'eu-west-2', as is not the default region we need to specify the
        # CreateBucketConfiguration parameter
        s3 = boto3.client("s3", region_name="eu-west-2")
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        s3.upload_file(
            str(sqlite3_path), bucket_name, f"{object_key}/{dataset}.sqlite3"
        )

        # Verify file is in mock S3
        response = s3.list_objects_v2(Bucket=bucket_name)
        assert "Contents" in response, "SQLite file should be present in mock S3"
        assert any(
            obj["Key"] == f"{object_key}/{dataset}.sqlite3"
            for obj in response["Contents"]
        )

        # Delete to ensure that we download the file from mock s3 (have keopt a copy earlier)
        os.remove(str(sqlite3_path))
        assert not os.path.exists(
            str(sqlite3_path)
        ), "sqlite3 file still exists after move to s3"

        # Keep mock AWS for later tests
        yield s3


def test_dataset_update_with_new_resources(
    test_dataset_create_fixture, test_mock_s3_bucket, test_dataset_update_fixture, temp_dir
):
    """
    Check that original sqlite3 file is created, then when we have new resources we create an
    updated sqlite3 file.
    """
    #  initial row counts
    orig_sqlite3_path = test_dataset_create_fixture["orig_sqlite3_path"]
    assert os.path.exists(orig_sqlite3_path), "Original sqlite3 file no longer exists"
    orig_conn = sqlite3.connect(orig_sqlite3_path)
    orig_cursor = orig_conn.cursor()
    initial_counts = get_table_row_counts(orig_cursor)

    # Updated row counts
    sqlite3_path = test_dataset_create_fixture["sqlite3_path"]
    assert os.path.exists(sqlite3_path), "Updated sqlite3 file no longer exists"
    updated_conn = sqlite3.connect(sqlite3_path)
    updated_cursor = updated_conn.cursor()
    updated_counts = get_table_row_counts(updated_cursor)

    assert set(initial_counts.keys()) == set(
        updated_counts.keys()
    ), "❌ Table names mismatch between initial and updated counts"

    for table in initial_counts:
        initial = initial_counts[table]
        updated = updated_counts.get(table)

        # Check more rows in updated table (where we have rows)
        if initial == 0:
            assert initial == updated, "One table empty but not the other"
        else:
            assert (
                updated > initial
            ), f"❌ {table}: Expected count to increase (Initial: {initial}, Updated: {updated})"

            # Check rows in original table in updated table
            orig_cursor.execute(f"SELECT * FROM {table}")
            orig_rows = orig_cursor.fetchall()

            updated_cursor.execute(f"SELECT * FROM {table}")
            updated_rows = set(updated_cursor.fetchall())  # Use set for quick lookup

            for row in orig_rows:
                assert (
                    row in updated_rows
                ), f"❌ Row {row} from table '{table}' is missing in the updated database!"

    orig_cursor.close()
    updated_cursor.close()
