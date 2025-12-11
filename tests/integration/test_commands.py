import pytest
import csv
import os
import sqlite3
import tempfile
import shutil
import boto3
import logging
import json
import urllib.request
import pandas as pd

from moto import mock_aws
from pathlib import Path
from csv import DictReader

from digital_land.commands import (
    dataset_dump_flattened,
    _create_parquet_from_csv,
    fetch,
)
from digital_land.package.dataset import DatasetPackage

from digital_land.specification import Specification
from digital_land.organisation import Organisation
from digital_land.collect import Collector
from digital_land.pipeline.main import Pipeline
import duckdb
import responses


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
        [
            "old-entity",
            "status",
            "entity",
            "end-date",
            "notes",
            "entry-date",
            "start-date",
        ],
        [44011900, 301, 44011910, "", "", "", ""],
    ]
    with open(csv_filename, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(data)
    csv_filename = pipeline_dir / "old-entity_updated.csv"
    data = [
        [
            "old-entity",
            "status",
            "entity",
            "end-date",
            "notes",
            "entry-date",
            "start-date",
        ],
        [44011900, 301, 44011910, "", "", "", ""],
        [44011903, 301, 44011913, "", "", "", ""],
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
        entity_no = 44011910 + 3 * (res != resources[0])
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

    old_entity_path = os.path.join(
        resource_files_fixture["pipeline_dir"], "old-entity.csv"
    )
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

    old_entity_path = os.path.join(
        resource_files_fixture["pipeline_dir"], "old-entity_updated.csv"
    )
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
    test_dataset_create_fixture,
    test_mock_s3_bucket,
    test_dataset_update_fixture,
    temp_dir,
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


def get_expected_fields(specification, dataset_name):
    """
    Helper function to get expected fields for a dataset from the specification. Should be replaced
    """
    expected_fields = specification.schema_field[dataset_name]
    # Remove 'organisation' field if it exists
    expected_fields = [field for field in expected_fields]
    # Add additional fields
    expected_fields.append("dataset")
    expected_fields.append("organisation-entity")
    expected_fields.append("geojson")
    if "geometry" not in expected_fields:
        expected_fields.append("geometry")
    if "point" not in expected_fields:
        expected_fields.append("point")
    expected_fields.append("typology")
    return expected_fields


@pytest.mark.parametrize(
    "dataset_name",
    ["listed-building-grade"],
)
def test_dataset_dump_flattened_for_non_geospatial_data(
    # Parametrize args
    dataset_name,
    # Runtime filesystem dependencies generated by previous steps
    pipeline_dir,
    # Test assertion directories
    dataset_dir,
    # Pytest fixtures
    tmp_path,
    specification_dir,
):
    # Setup
    expected_flattened_csv_result = dataset_dir.joinpath(f"{dataset_name}-expected.csv")

    csv_path = dataset_dir.joinpath(f"{dataset_name}.csv")

    flattened_output_dir = tmp_path.joinpath("dataset_output")
    flattened_output_dir.mkdir()
    flattened_csv_path = flattened_output_dir.joinpath(f"{dataset_name}.csv")
    flattened_json_path = flattened_output_dir.joinpath(f"{dataset_name}.json")

    specification = Specification(specification_dir)

    dataset_dump_flattened(csv_path, flattened_output_dir, specification, dataset_name)

    # This is bas because if the specification changes then it will error
    with flattened_csv_path.open() as actual, expected_flattened_csv_result.open() as expected:
        actual_dict_reader = DictReader(actual)
        expected_dict_reader = DictReader(expected)
        expected_fields = get_expected_fields(specification, dataset_name)
        assert sorted(actual_dict_reader.fieldnames) == sorted(expected_fields)
        for row_actual, row_expected in zip(actual_dict_reader, expected_dict_reader):
            check_fields = [
                field
                for field in expected_fields
                if field in expected_dict_reader.fieldnames
            ]
            for field in check_fields:
                assert (
                    row_actual[field] == row_expected[field]
                ), f"Field '{field}' mismatch in row {row_actual}"

    assert os.path.isfile(flattened_json_path)
    with flattened_json_path.open() as actual:
        data = json.load(actual)
        assert 3 == len(data["entities"])


@pytest.mark.parametrize(
    "dataset_name",
    ["listed-building-outline"],
)
def test_dataset_dump_flattened_for_geospatial_data(
    # Parametrize args
    dataset_name,
    # Runtime filesystem dependencies generated by previous steps
    pipeline_dir,
    # Test assertion directories
    dataset_dir,
    # Pytest fixtures
    tmp_path,
    specification_dir,
):
    # Setup
    expected_flattened_csv_result = dataset_dir.joinpath(f"{dataset_name}-expected.csv")

    csv_path = dataset_dir.joinpath(f"{dataset_name}.csv")

    flattened_output_dir = tmp_path.joinpath("dataset_output")
    flattened_output_dir.mkdir(exist_ok=True)
    flattened_csv_path = flattened_output_dir.joinpath(f"{dataset_name}.csv")
    flattened_json_path = flattened_output_dir.joinpath(f"{dataset_name}.json")

    specification = Specification(specification_dir)

    dataset_dump_flattened(csv_path, flattened_output_dir, specification, dataset_name)

    with flattened_csv_path.open() as actual, expected_flattened_csv_result.open() as expected:
        actual_dict_reader = DictReader(actual)
        expected_dict_reader = DictReader(expected)
        expected_fields = expected_fields = get_expected_fields(
            specification, dataset_name
        )
        assert sorted(actual_dict_reader.fieldnames) == sorted(expected_fields)
        for row_actual, row_expected in zip(actual_dict_reader, expected_dict_reader):
            check_fields = [
                field
                for field in expected_fields
                if field in expected_dict_reader.fieldnames
            ]
            for field in check_fields:
                assert (
                    row_actual[field] == row_expected[field]
                ), f"Field '{field}' mismatch in row {row_actual}"

    assert os.path.isfile(flattened_json_path)
    with flattened_json_path.open() as actual:
        data = json.load(actual)
        assert 1 == len(data["entities"])


def test_collection_dir_file_hashes(temp_dir, caplog):
    """
    Test that log_file_hashes returns the correct hashes
    """
    source = temp_dir / "source.csv"
    endpoint = temp_dir / "endpoint.csv"
    source.write_text("id,value\n1,100\n")
    endpoint.write_text("id,value\n2,200\n")

    expected_logs = [
        "CSV file: source.csv | hash: 2bab99eb38f4003b26453326912159db879c451a",
        "CSV file: endpoint.csv | hash: 53fccae46018c7d10759987090b904e41a1d9092",
    ]
    caplog.set_level(logging.INFO)
    # Create and run Collector
    collector = Collector(
        resource_dir=str(temp_dir / "resource"), log_dir=str(temp_dir / "log")
    )
    collector.collection_dir_file_hashes(temp_dir)

    for expected in expected_logs:
        assert any(
            expected in record.message for record in caplog.records
        ), f"Missing log: {expected}"


def test_create_parquet_from_csv_creates_parquet_file(tmp_path, specification_dir):
    """Test that _create_parquet_from_csv creates a parquet file from a CSV"""
    # Setup
    dataset_name = "test-dataset"
    csv_path = tmp_path / f"{dataset_name}.csv"

    # Create a test CSV file
    csv_content = """entity,name,start-date,reference,entry-date
1,Test Name,2024-01-01,REF001,2024-01-01
2,Another Test,2024-02-15,REF002,2024-02-15"""
    csv_path.write_text(csv_content)

    # Create a mock specification
    specification = Specification(specification_dir)
    field_names = ["entity", "name", "start-date", "reference", "entry-date"]

    # Execute
    _create_parquet_from_csv(
        str(csv_path), str(tmp_path), dataset_name, specification, field_names
    )

    # Assert parquet file exists
    parquet_path = tmp_path / f"{dataset_name}.parquet"
    assert parquet_path.exists(), "Parquet file should be created"


def test_create_parquet_from_csv_preserves_data_types(tmp_path, specification_dir):
    """Test that _create_parquet_from_csv correctly types fields based on specification"""
    # Setup
    dataset_name = "test-dataset"
    csv_path = tmp_path / f"{dataset_name}.csv"

    # Create a test CSV with various data types
    csv_content = """entity,name,start-date,reference
123,Test Name,2024-01-01,REF001
456,Another Test,2024-02-15,REF002"""
    csv_path.write_text(csv_content)

    specification = Specification(specification_dir)
    field_names = ["entity", "name", "start-date", "reference"]

    # Execute
    _create_parquet_from_csv(
        str(csv_path), str(tmp_path), dataset_name, specification, field_names
    )

    # Assert - Read parquet and check data types
    parquet_path = tmp_path / f"{dataset_name}.parquet"
    conn = duckdb.connect()
    result = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}')").fetchall()

    # Verify data was loaded
    assert len(result) == 2, "Should have 2 rows"
    assert result[0][0] == 123, "Entity should be integer"
    assert result[0][1] == "Test Name", "Name should be string"

    # Check schema types
    schema = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
    ).fetchall()
    schema_dict = {row[0]: row[1] for row in schema}

    # entity should be BIGINT (integer type)
    assert "BIGINT" in schema_dict.get("entity", ""), "Entity should be BIGINT type"
    # name should be VARCHAR (string type)
    assert "VARCHAR" in schema_dict.get("name", ""), "Name should be VARCHAR type"

    conn.close()


def test_create_parquet_from_csv_handles_empty_strings(tmp_path, specification_dir):
    """Test that _create_parquet_from_csv converts empty strings to NULL for numeric types"""
    # Setup
    dataset_name = "test-dataset"
    csv_path = tmp_path / f"{dataset_name}.csv"

    # Create CSV with empty strings
    csv_content = """entity,name,start-date
123,Test Name,2024-01-01
456,,"""
    csv_path.write_text(csv_content)

    specification = Specification(specification_dir)
    field_names = ["entity", "name", "start-date"]

    # Execute
    _create_parquet_from_csv(
        str(csv_path), str(tmp_path), dataset_name, specification, field_names
    )

    # Assert - Check that empty strings are handled properly
    parquet_path = tmp_path / f"{dataset_name}.parquet"
    conn = duckdb.connect()
    result = conn.execute(f"SELECT * FROM read_parquet('{parquet_path}')").fetchall()

    assert len(result) == 2, "Should have 2 rows"
    # Second row should have NULL for empty date field
    assert result[1][2] is None, "Empty date string should be converted to NULL"

    conn.close()


def test_create_parquet_from_csv_raises_on_unexpected_errors(
    tmp_path, specification_dir
):
    """Test that _create_parquet_from_csv raises exceptions for unexpected errors"""
    # Setup with invalid CSV path
    dataset_name = "test-dataset"
    csv_path = tmp_path / "nonexistent.csv"  # File doesn't exist

    specification = Specification(specification_dir)
    field_names = ["entity", "name"]

    # Execute & Assert - Should raise an exception
    with pytest.raises(Exception):
        _create_parquet_from_csv(
            str(csv_path), str(tmp_path), dataset_name, specification, field_names
        )


def test_create_parquet_from_csv_errors_on_invalid_data(tmp_path, specification_dir):
    """Test that _create_parquet_from_csv raises error when data doesn't match expected types"""
    # Setup with invalid data
    dataset_name = "test-dataset"
    csv_path = tmp_path / f"{dataset_name}.csv"

    # Create CSV with invalid integer value
    csv_content = """entity,name
123,Valid Name
not-a-number,Another Name"""
    csv_path.write_text(csv_content)

    specification = Specification(specification_dir)
    field_names = ["entity", "name"]

    # Execute & Assert - Should raise an exception due to type mismatch
    with pytest.raises(Exception):
        _create_parquet_from_csv(
            str(csv_path), str(tmp_path), dataset_name, specification, field_names
        )


def test_create_parquet_from_csv_handles_large_multipolygon(
    tmp_path, specification_dir
):
    """Test that _create_parquet_from_csv can handle very large multipolygon/geometry columns"""
    # Setup
    dataset_name = "test-dataset"
    csv_path = tmp_path / f"{dataset_name}.csv"

    # Create a large multipolygon string (simulate 1MB of WKT data)
    # This represents a complex geometry with many coordinates
    large_multipolygon = (
        "MULTIPOLYGON(((" + ",".join([f"{i} {i}" for i in range(100000)]) + ")))"
    )

    # Write CSV with proper quoting using csv module to handle large fields correctly
    import csv as csv_module

    with open(csv_path, "w", newline="") as f:
        writer = csv_module.writer(f)
        writer.writerow(["entity", "geometry"])
        writer.writerow(["123", large_multipolygon])
        writer.writerow(["456", "POINT(1 1)"])

    specification = Specification(specification_dir)
    field_names = ["entity", "geometry"]

    # Execute
    _create_parquet_from_csv(
        str(csv_path), str(tmp_path), dataset_name, specification, field_names
    )

    # Assert - Verify parquet file was created and contains the large geometry
    parquet_path = tmp_path / f"{dataset_name}.parquet"
    assert parquet_path.exists(), "Parquet file should be created"

    # Read back and verify the large geometry was preserved
    conn = duckdb.connect()
    result = conn.execute(
        f"SELECT entity, length(geometry) FROM read_parquet('{parquet_path}') WHERE entity = 123"
    ).fetchone()

    assert result[0] == 123, "Entity should match"
    assert result[1] == len(
        large_multipolygon
    ), f"Geometry length should be preserved ({len(large_multipolygon):,} bytes)"

    conn.close()


def test_dataset_dump_flattened_creates_parquet_file(
    dataset_dir, tmp_path, specification_dir
):
    """Test that dataset_dump_flattened creates a parquet file alongside CSV and JSON"""
    # Setup
    dataset_name = "listed-building-grade"
    csv_path = dataset_dir.joinpath(f"{dataset_name}.csv")

    flattened_output_dir = tmp_path.joinpath("dataset_output")
    flattened_output_dir.mkdir()

    specification = Specification(specification_dir)

    # Execute
    dataset_dump_flattened(csv_path, flattened_output_dir, specification, dataset_name)

    # Assert - Check all output files exist
    flattened_csv_path = flattened_output_dir.joinpath(f"{dataset_name}.csv")
    flattened_json_path = flattened_output_dir.joinpath(f"{dataset_name}.json")
    flattened_parquet_path = flattened_output_dir.joinpath(f"{dataset_name}.parquet")

    assert flattened_csv_path.exists(), "CSV file should be created"
    assert flattened_json_path.exists(), "JSON file should be created"
    assert flattened_parquet_path.exists(), "Parquet file should be created"


def test_dataset_dump_flattened_parquet_matches_csv_data(
    dataset_dir, tmp_path, specification_dir
):
    """Test that parquet file created by dataset_dump_flattened contains same data as CSV"""
    # Setup
    dataset_name = "listed-building-grade"
    csv_path = dataset_dir.joinpath(f"{dataset_name}.csv")

    flattened_output_dir = tmp_path.joinpath("dataset_output")
    flattened_output_dir.mkdir()

    specification = Specification(specification_dir)

    # Execute
    dataset_dump_flattened(csv_path, flattened_output_dir, specification, dataset_name)

    # Assert - Compare data between CSV and parquet
    flattened_csv_path = flattened_output_dir.joinpath(f"{dataset_name}.csv")
    flattened_parquet_path = flattened_output_dir.joinpath(f"{dataset_name}.parquet")

    # Read CSV
    with open(flattened_csv_path, "r") as f:
        csv_reader = DictReader(f)
        csv_rows = list(csv_reader)

    # Read parquet
    conn = duckdb.connect()
    parquet_rows = conn.execute(
        f"SELECT * FROM read_parquet('{flattened_parquet_path}')"
    ).fetchall()

    # Get column names from parquet
    columns = [
        desc[0]
        for desc in conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{flattened_parquet_path}')"
        ).fetchall()
    ]

    conn.close()

    # Compare row counts
    assert len(csv_rows) == len(
        parquet_rows
    ), "CSV and parquet should have same number of rows"

    # Compare first row entity values
    csv_entity = csv_rows[0].get("entity", "")
    parquet_entity = (
        str(parquet_rows[0][columns.index("entity")]) if "entity" in columns else ""
    )

    assert (
        csv_entity == parquet_entity
    ), "Entity values should match between CSV and parquet"


def test_dataset_dump_flattened_parquet_has_correct_schema(
    dataset_dir, tmp_path, specification_dir
):
    """Test that parquet file has correct column types based on specification"""
    # Setup
    dataset_name = "listed-building-grade"
    csv_path = dataset_dir.joinpath(f"{dataset_name}.csv")

    flattened_output_dir = tmp_path.joinpath("dataset_output")
    flattened_output_dir.mkdir()

    specification = Specification(specification_dir)

    # Execute
    dataset_dump_flattened(csv_path, flattened_output_dir, specification, dataset_name)

    # Assert - Check schema
    flattened_parquet_path = flattened_output_dir.joinpath(f"{dataset_name}.parquet")

    conn = duckdb.connect()
    schema = conn.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{flattened_parquet_path}')"
    ).fetchall()

    schema_dict = {row[0]: row[1] for row in schema}

    # Check that entity field is typed as integer
    if "entity" in schema_dict:
        assert "BIGINT" in schema_dict["entity"], "Entity should be BIGINT type"

    # Check that string fields are VARCHAR
    if "name" in schema_dict:
        assert "VARCHAR" in schema_dict["name"], "Name should be VARCHAR type"

    conn.close()


@responses.activate
def test_fetch_creates_resource_file(tmp_path):
    """Test that fetch command creates resource file in collection/resource/ directory"""
    url = "http://example.com/data.csv"
    responses.add(responses.GET, url, body="test data content", status=200)

    # Create minimal pipeline structure
    pipeline_dir = tmp_path / "pipeline" / "test-dataset"
    pipeline_dir.mkdir(parents=True)
    pipeline = Pipeline(str(tmp_path / "pipeline"), "test-dataset")

    # Change to tmp directory for test
    original_dir = os.getcwd()
    os.chdir(tmp_path)

    try:
        fetch(url, pipeline)

        # Check that resource directory was created in default location
        resource_dir = tmp_path / "collection" / "resource"
        assert resource_dir.exists(), "Resource directory should be created"

        # Check that exactly one resource file was created
        resource_files = list(resource_dir.glob("*"))
        assert len(resource_files) == 1, "Exactly one resource file should be created"

        # Verify the content matches what was fetched
        resource_file = resource_files[0]
        with open(resource_file, "r") as f:
            content = f.read()
        assert (
            content == "test data content"
        ), "Resource file should contain fetched content"

    finally:
        os.chdir(original_dir)
