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

from digital_land.package.dataset import DatasetPackage
from digital_land.package.package import Specification
from digital_land.organisation import Organisation


@pytest.fixture
def transformed_fact_resources():
    input_data = [
        {
            "entity": "44006677",
            "entry-date": "2021-09-06",
            "fact": "1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3",
            "field": "name",
            "value": "Burghwallis",
        },
        {
            "entity": "44006677",
            "entry-date": "2022-11-02",
            "fact": "1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3",
            "field": "name",
            "value": "Burghwallis",
        },
    ]

    return input_data


@pytest.fixture
def transformed_fact_resources_with_blank():
    input_data = [
        {
            "entity": "44006677",
            "entry-date": "2021-09-06",
            "fact": "1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3",
            "field": "name",
            "value": "Burghwallis",
            "end-date": "2021-12-31",
        },
        {
            "entity": "44006677",
            "entry-date": "2022-11-02",
            "fact": "1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3",
            "field": "name",
            "value": "Burghwallis",
            "end-date": "",
        },
    ]

    return input_data


@pytest.fixture
def organisation_csv(tmp_path):
    organisation_path = os.path.join(tmp_path, "organisation.csv")
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv",
        organisation_path,
    )
    return organisation_path


@pytest.fixture
def blank_patch_csv(tmp_path):
    patch_path = os.path.join(tmp_path, "organisation.csv")
    fieldnames = ["dataset", "resource", "field", "pattern", "value"]
    with open(patch_path, "w", encoding="UTF8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
    return patch_path


def test_load_old_entities_entities_outside_of_range_are_removed(tmp_path):
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

    specification = Specification(schema=schema, field=field)
    organisation = Organisation(
        organisation_path=None, pipeline_dir=None, organisation={}
    )

    # write data to csv as we only seem to load from csv
    data = [
        {
            "end-date": "",
            "entity": "",
            "entry-date": "",
            "notes": "",
            "old-entity": "1",
            "start-date": "",
            "status": "410",
        },
        {
            "end-date": "",
            "entity": "",
            "entry-date": "",
            "notes": "",
            "old-entity": "3",
            "start-date": "",
            "status": "410",
        },
    ]

    old_entity_path = os.path.join(tmp_path, "old-entity.csv")
    with open(old_entity_path, "w") as f:  # You will need 'wb' mode in Python 2.x
        w = csv.DictWriter(f, data[0].keys())
        w.writeheader()
        w.writerows(data)

    # create sqlite db
    sqlite3_path = os.path.join(tmp_path, "test.sqlite3")

    # create class on sqlite db with old_entity table in it
    package = DatasetPackage(
        "conservation-area",
        organisation=organisation,
        path=sqlite3_path,
        specification=specification,
    )
    package.connect()
    package.create_cursor()
    package.create_table("old-entity", schema["old-entity"]["fields"], "old-entity")

    # run load old_entity function from csv above
    package.load_old_entities(old_entity_path)
    package.disconnect()
    # test entity out of range is not in sqlite
    with sqlite3.connect(sqlite3_path) as con:
        sql = """
        SELECT * FROM old_entity;
        """
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        results = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

    assert results["old_entity"].max() <= 2
    assert results["old_entity"].min() >= 1


def test_entry_date_upsert_uploads_newest_date(
    specification_dir,
    organisation_csv,
    blank_patch_csv,
    transformed_fact_resources,
    tmp_path,
):
    dataset = "conservation-area"
    sqlite3_path = os.path.join(tmp_path, f"{dataset}.sqlite3")

    organisation = Organisation(
        organisation_path=organisation_csv,
        pipeline_dir=Path(os.path.dirname(blank_patch_csv)),
    )
    package = DatasetPackage(
        "conservation-area",
        organisation=organisation,
        path=sqlite3_path,
        specification_dir=specification_dir,  # TBD: package should use this specification object
    )

    # create package
    package.create()

    # run upload to fact table not fact resource for testing the upsert
    package.connect()
    package.create_cursor()
    fact_fields = package.specification.schema["fact"]["fields"]
    fact_conflict_fields = ["fact"]
    fact_update_fields = [
        field for field in fact_fields if field not in fact_conflict_fields
    ]
    for row in transformed_fact_resources:
        package.entry_date_upsert(
            "fact", fact_fields, row, fact_conflict_fields, fact_update_fields
        )
    package.commit()
    package.disconnect()

    # retrieve results
    package.connect()
    package.create_cursor()
    package.cursor.execute("SELECT * FROM fact;")
    cols = [column[0] for column in package.cursor.description]
    actual_result = pd.DataFrame.from_records(
        package.cursor.fetchall(), columns=cols
    ).to_dict(orient="records")
    expected_result = [
        {
            "end_date": "",
            "entity": 44006677,
            "fact": "1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3",
            "field": "name",
            "priority": None,
            "entry_date": "2022-11-02",
            "reference_entity": "",
            "start_date": "",
            "value": "Burghwallis",
        }
    ]

    assert actual_result == expected_result, "actual result does not match query"


def test_load_issues_uploads_issues_from_csv(tmp_path):
    # create custom specification to feed in
    schema = {
        "issue": {
            "fields": [
                "end-date",
                "entry-date",
                "entry-number",
                "field",
                "issue-type",
                "line-number",
                "dataset",
                "resource",
                "start-date",
                "value",
            ]
        },
        "conservation-area": {"entity-minimum": "1", "entity-maximum": "2"},
        "entity": {"fields": []},
    }
    field = {
        "end-date": {
            "datatype": "datetime",
        },
        "entry-date": {"datatype": "datetime"},
        "entry-number": {"datatype": "integer"},
        "field": {"datatype": "string"},
        "issue-type": {
            "datatype": "string",
        },
        "line-number": {
            "datatype": "datetime",
        },
        "dataset": {
            "datatype": "string",
        },
        "resource": {
            "datatype": "string",
        },
        "start-date": {
            "datatype": "datetime",
        },
        "value": {
            "datatype": "text",
        },
    }

    specification = Specification(schema=schema, field=field)
    organisation = Organisation(
        organisation_path=None, pipeline_dir=None, organisation={}
    )

    # write data to csv as we only seem to load from csv
    data = [
        {
            "end-date": "",
            "entry-date": "",
            "entry-number": "1",
            "field": "test",
            "issue-type": "test",
            "line-number": "2",
            "dataset": "conservation-area",
            "resource": "efdec",
            "start-date": "",
            "value": "test",
        },
    ]

    issue_path = os.path.join(tmp_path, "efdec.csv")
    with open(issue_path, "w") as f:  # You will need 'wb' mode in Python 2.x
        w = csv.DictWriter(f, data[0].keys())
        w.writeheader()
        w.writerows(data)

    # create sqlite db
    sqlite3_path = os.path.join(tmp_path, "test.sqlite3")

    # create class on sqlite db with old_entity table in it
    package = DatasetPackage(
        "conservation-area",
        organisation=organisation,
        path=sqlite3_path,
        specification=specification,
    )
    package.connect()
    package.create_cursor()
    package.create_table("issue", schema["issue"]["fields"], "issue")

    # run load old_entity function from csv above
    package.load_issues(issue_path)
    package.disconnect()
    # test entity out of range is not in sqlite
    with sqlite3.connect(sqlite3_path) as con:
        sql = """
        SELECT * FROM issue;
        """
        cursor = con.execute(sql)
        cols = [column[0] for column in cursor.description]
        results = pd.DataFrame.from_records(data=cursor.fetchall(), columns=cols)

    assert len(results) > 0


def test_entry_date_upsert_uploads_blank_fields(
    specification_dir,
    organisation_csv,
    blank_patch_csv,
    transformed_fact_resources_with_blank,
    tmp_path,
):
    dataset = "conservation-area"
    sqlite3_path = os.path.join(tmp_path, f"{dataset}.sqlite3")

    organisation = Organisation(
        organisation_path=organisation_csv,
        pipeline_dir=Path(os.path.dirname(blank_patch_csv)),
    )
    package = DatasetPackage(
        "conservation-area",
        organisation=organisation,
        path=sqlite3_path,
        specification_dir=specification_dir,  # TBD: package should use this specification object
    )

    # create package
    package.create()

    # run upload to fact table not fact resource for testing the upsert
    package.connect()
    package.create_cursor()
    fact_fields = package.specification.schema["fact"]["fields"]
    fact_conflict_fields = ["fact"]
    fact_update_fields = [
        field for field in fact_fields if field not in fact_conflict_fields
    ]
    for row in transformed_fact_resources_with_blank:
        package.entry_date_upsert(
            "fact", fact_fields, row, fact_conflict_fields, fact_update_fields
        )
    package.commit()
    package.disconnect()

    # retrieve results
    package.connect()
    package.create_cursor()
    package.cursor.execute("SELECT * FROM fact;")
    cols = [column[0] for column in package.cursor.description]
    actual_result = pd.DataFrame.from_records(
        package.cursor.fetchall(), columns=cols
    ).to_dict(orient="records")
    expected_result = [
        {
            "end_date": "",
            "entity": 44006677,
            "fact": "1f90248fd06e49accd42b80e43d58beeac300f942f1a9f71da4b64865356b1f3",
            "field": "name",
            "priority": None,
            "entry_date": "2022-11-02",
            "reference_entity": "",
            "start_date": "",
            "value": "Burghwallis",
        }
    ]

    assert actual_result == expected_result, "actual result does not match query"


# Tests associated with the dataset_update function
# Currently `tmp_path` only has a function scope so need a separate one with a session scope
@pytest.fixture(scope="session")
def temp_dir():
    """Create a session-scoped temporary directory."""
    temp_directory = Path(tempfile.mkdtemp())  # Create a temp directory
    yield temp_directory
    shutil.rmtree(temp_directory)  # Cleanup after session


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
def transformed_data_fixture():
    """Provides current and new resource data."""
    return {
        "current": [
            {
                "end-date": "",
                "entity": "44011915",
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

    transformed_dir.mkdir(parents=True, exist_ok=True)
    column_field_dir.mkdir(parents=True, exist_ok=True)
    dataset_resource_dir.mkdir(parents=True, exist_ok=True)

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

    return {
        "transformed_dir": transformed_dir,
        "column_field_dir": column_field_dir,
        "dataset_resource_dir": dataset_resource_dir,
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
def sqlite_create_fixture(
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
def sqlite_update_fixture(
    sqlite_create_fixture,
    specification_dir,
    resource_files_fixture,
    organisation_csv_new_resources,
    blank_patch_csv_new_resources,
    temp_dir,
):
    """Creates and initializes the SQLite3 file with resources."""
    dataset = sqlite_create_fixture["dataset"]
    sqlite3_path = sqlite_create_fixture["sqlite3_path"]

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

    assert sqlite3_path.exists(), "SQLite file should exist after setup"

    # Fetch data for validation
    package.connect()
    package.create_cursor()
    package.cursor.execute("SELECT * FROM fact;")

    # cols = [column[0] for column in package.cursor.description]
    # actual_result = pd.DataFrame.from_records(
    #     package.cursor.fetchall(), columns=cols
    # ).to_dict(orient="records")
    # print(actual_result)
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


def test_create_tables_fixture(sqlite_create_fixture):
    """Need to set up an initial sqlite3 file before sending it to S3 and then jupdating it."""
    package = sqlite_create_fixture["package"]
    sqlite3_path = sqlite_create_fixture["sqlite3_path"]

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
        sqlite_create_fixture["actual_result"] == orig_values["fact"]
    ), "results differ across scopes"


@pytest.fixture(scope="session")
def test_mock_s3_bucket(sqlite_create_fixture):
    sqlite3_path = sqlite_create_fixture["sqlite3_path"]
    dataset = sqlite_create_fixture["dataset"]
    bucket_name = "test-collection-data"
    object_key = f"test-collection/{dataset}"

    print("Running test_mock_s3_bucket...")

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


def test_update_tables_with_new_resources(
    sqlite_create_fixture, test_mock_s3_bucket, sqlite_update_fixture, temp_dir
):
    #  initial row counts
    assert os.path.exists(
        sqlite_create_fixture["orig_sqlite3_path"]
    ), "Original sqlite3 file no longer exists"
    conn = sqlite3.connect(sqlite_create_fixture["orig_sqlite3_path"])
    cursor = conn.cursor()
    initial_counts = get_table_row_counts(cursor)
    conn.close()

    sqlite3_path = sqlite_create_fixture["sqlite3_path"]
    assert os.path.exists(sqlite3_path), "Updated sqlite3 file no longer exists"
    conn = sqlite3.connect(sqlite3_path)
    cursor = conn.cursor()
    updated_counts = get_table_row_counts(cursor)
    conn.close()

    assert set(initial_counts.keys()) == set(
        updated_counts.keys()
    ), "❌ Table names mismatch between initial and updated counts"

    for table in initial_counts:
        initial = initial_counts[table]
        updated = updated_counts.get(table)

        if initial == 0:
            assert initial == updated, "One table empty but not the other"
        else:
            assert (
                updated > initial
            ), f"❌ {table}: Expected count to increase (Initial: {initial}, Updated: {updated})"
