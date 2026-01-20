"""
These tests are partially fragile (rely on external data) but aim to run the transform function of the pipeline with real input data collected from S3,
then to compare key outputs to what is expected and some comparison to the actual stored transformed data.

Code to initalise transfrom is similar to jupyter notebook debug_resource_transformation.ipynb.

NOTE: If transform logic changes, then approve with integration tests first and then update expected transformed data resource hash in RESOUCES to those in S3 accordingly.
"""

import os
import urllib.request
from pathlib import Path
import pandas as pd
import pytest

from digital_land.collection import Collection
from digital_land.configuration.main import Config
from digital_land.organisation import Organisation
from digital_land.pipeline import Pipeline
from digital_land.specification import Specification
from digital_land.api import API
from digital_land.phase.convert import detect_file_encoding

data_collection_url = "https://files.planning.data.gov.uk/"

RESOURCES = [
    (
        "ca66c573ea73ceaddef3188467ec69a554bd215004ba5603f045337803813c58",  # Salford
        "article-4-direction",
    ),
    (
        "1c192f194a6d7cb044006bbe0d7bb7909eed3783eeb8a53026fc15b9fe31a836",  # Camden
        "article-4-direction-area",
    ),
    (
        "3d2d3ec4ce9e87ab348a85e181509d2ed8ebb39ea43594fb0e7a55b3e703f919",  # Lambeth
        "conservation-area",
    ),
]


def _download_real_resource_and_transform(test_dirs, resource_hash, dataset):
    # Directories and File Paths Needed
    specification_dir = test_dirs["specification_dir"]
    pipeline_dir = test_dirs["pipeline_dir"]
    collection_dir = test_dirs["collection_dir"]
    cache_dir = test_dirs[
        "cache_dir"
    ]  # Cache stores the config sqlite and organisation data
    config_path = cache_dir / "config.sqlite3"
    converted_path = test_dirs["converted_resource_dir"] / f"{resource_hash}.csv"
    org_path = cache_dir / "organisation.csv"
    output_path = test_dirs["transformed_dir"] / f"{resource_hash}.csv"
    resource_path = test_dirs["collection_dir"] / "resource" / f"{resource_hash}"
    expected_transformed_path = (
        test_dirs["transformed_dir"] / f"{resource_hash}-expected.csv"
    )

    specification_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    collection_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    converted_path.parent.mkdir(parents=True, exist_ok=True)
    Path(resource_path).parent.mkdir(parents=True, exist_ok=True)

    # Specification Setup
    Specification.download(specification_dir)
    spec = Specification(specification_dir)
    collection = spec.dataset[dataset]["collection"]
    if not collection:
        raise ValueError(
            f"Dataset {dataset} does not have a collection defined in the specification"
        )

    # URLs for resource data and expected transformed data - Need to know collection and dataset
    resource_url = f"{data_collection_url}{collection}-collection/collection/resource/{resource_hash}"
    expected_transformed_url = f"{data_collection_url}{collection}-collection/transformed/{dataset}/{resource_hash}.csv"

    # Download the configuration/pipeline files for that collection
    Config.download_pipeline_files(path=pipeline_dir, collection=collection)

    # Download the Raw Collect Resource Data from Planning Files
    if not os.path.exists(resource_path):
        print(f"Downloading {resource_url} to {resource_path}")
        urllib.request.urlretrieve(resource_url, resource_path)
    else:
        print(f"Using existing file {resource_path}")

    # Download Collection Files (need to know the endpoint hash for the resource)
    Collection.download(path=collection_dir, collection=collection)

    # Download Organisation data
    Organisation.download(path=org_path)

    # Collection data download TODO include redirects
    collection = Collection(directory=test_dirs["collection_dir"])
    collection.load()
    endpoints = collection.resource_endpoints(resource_hash)
    organisations = collection.resource_organisations(resource_hash)
    entry_date = collection.resource_start_date(resource_hash)

    # Config Setup
    config = Config(path=config_path, specification=spec)
    config.create()
    tables = {key: pipeline_dir for key in config.tables.keys()}
    config.load(tables)

    # Pipeline and Organisation Init
    pipeline = Pipeline(pipeline_dir, dataset, specification=spec, config=config)
    organisation = Organisation(org_path, Path(pipeline.path))

    # Api to get valid category values
    api = API(specification=spec)
    valid_category_values = api.get_valid_category_values(dataset, pipeline)

    pipeline.transform(
        resource=resource_hash,
        organisation=organisation,
        organisations=organisations,
        endpoints=endpoints,
        input_path=resource_path,
        output_path=output_path,
        entry_date=entry_date,
        converted_path=converted_path,
        valid_category_values=valid_category_values,
    )

    # Save logs in pipeline
    pipeline.save_logs(
        issue_path=os.path.join(test_dirs["issues_log_dir"], resource_hash + ".csv"),
        operational_issue_path=os.path.join(
            test_dirs["operational_issues_dir"], resource_hash + ".csv"
        ),
        column_field_path=os.path.join(
            test_dirs["column_field_dir"], resource_hash + ".csv"
        ),
        dataset_resource_path=os.path.join(
            test_dirs["dataset_resource_dir"], resource_hash + ".csv"
        ),
    )

    # Finally store the acutal transformed data from Planning Files for some comparison
    if not expected_transformed_path.exists():
        print(f"Downloading {expected_transformed_url} to {expected_transformed_path}")
        urllib.request.urlretrieve(expected_transformed_url, expected_transformed_path)


@pytest.fixture(scope="module", params=RESOURCES)
def transformed_resource(request, test_dirs):
    resource_hash, dataset = request.param
    _download_real_resource_and_transform(test_dirs, resource_hash, dataset)

    converted_resource_path = (
        test_dirs["converted_resource_dir"] / f"{resource_hash}.csv"
    )
    resource_path = test_dirs["collection_dir"] / "resource" / f"{resource_hash}"

    # Use converted resource if it exists, otherwise use original resource (for CSV files that don't need conversion)
    if converted_resource_path.exists():
        source_path = converted_resource_path
    else:
        source_path = resource_path

    return {
        "resource_hash": resource_hash,
        "dataset": dataset,
        "transformed_path": test_dirs["transformed_dir"] / f"{resource_hash}.csv",
        "expected_transformed_path": test_dirs["transformed_dir"]
        / f"{resource_hash}-expected.csv",
        "resource_path": resource_path,
        "source_path": source_path,
        "issue_path": test_dirs["issues_log_dir"] / f"{resource_hash}.csv",
        "operational_issue_path": test_dirs["operational_issues_dir"]
        / f"{resource_hash}.csv",
        "column_field_path": test_dirs["column_field_dir"] / f"{resource_hash}.csv",
        "dataset_resource_path": test_dirs["dataset_resource_dir"]
        / f"{resource_hash}.csv",
        "converted_resource_path": converted_resource_path,
    }


# Test that all expected files are created
def test_files_created(transformed_resource):
    transformed_path = transformed_resource["transformed_path"]
    expected_transformed_path = transformed_resource["expected_transformed_path"]
    issue_path = transformed_resource["issue_path"]
    operational_issue_path = transformed_resource["operational_issue_path"]
    column_field_path = transformed_resource["column_field_path"]
    dataset_resource_path = transformed_resource["dataset_resource_path"]
    source_path = transformed_resource["source_path"]

    # Detect encoding for source file to handle non-UTF8 encodings
    detected_encoding = detect_file_encoding(source_path)
    if not detected_encoding:
        detected_encoding = "utf-8"

    assert (
        transformed_path.exists()
    ), f"Transformed file {transformed_path} does not exist"
    assert (
        expected_transformed_path.exists()
    ), f"Expected transformed file {expected_transformed_path} does not exist"
    assert issue_path.exists(), f"Issue log file {issue_path} does not exist"
    assert (
        operational_issue_path.exists()
    ), f"Operational issue log file {operational_issue_path} does not exist"
    assert (
        column_field_path.exists()
    ), f"Column field log file {column_field_path} does not exist"
    assert (
        dataset_resource_path.exists()
    ), f"Dataset resource log file {dataset_resource_path} does not exist"


# Test that some basic properties of the transformed data are correct
def test_transform_basic(transformed_resource):
    transformed_path = transformed_resource["transformed_path"]
    output_df = pd.read_csv(transformed_path)

    assert len(output_df) > 0, "Transformed output should have at least one row"

    assert "resource" in output_df.columns, "Output must have 'resource' column"
    assert output_df["resource"].notna().all(), "All rows should have a resource value"

    # Check that field column contains expected dataset-specific fields
    assert "field" in output_df.columns, "Output must have 'field' column"
    fields = output_df["field"].unique()
    assert len(fields) > 0, "Should have at least one field type"

    # Check long format structure - multiple rows per entity (one per field)
    entities = output_df["entity"].unique()
    if len(entities) > 0:
        sample_entity = entities[0]
        entity_rows = output_df[output_df["entity"] == sample_entity]
        assert (
            len(entity_rows) > 1
        ), f"Entity {sample_entity} should have multiple rows (one per field)"


# Test that expected columns exist in the transformed output
def test_expected_columns_exist(transformed_resource):
    transformed_path = transformed_resource["transformed_path"]
    expected_transformed_path = transformed_resource["expected_transformed_path"]

    output_df = pd.read_csv(transformed_path)
    expected_output_df = pd.read_csv(expected_transformed_path)

    # Get columns from expected output
    expected_columns = set(expected_output_df.columns)
    output_columns = set(output_df.columns)

    # Check that all expected columns exist in our output
    missing_columns = expected_columns - output_columns
    assert len(missing_columns) == 0, f"Missing expected columns: {missing_columns}"

    # Check for unexpected extra columns
    extra_columns = output_columns - expected_columns
    assert len(extra_columns) == 0, f"Unexpected extra columns: {extra_columns}"

    # Verify critical long-format columns are present
    critical_columns = {
        "entity",
        "entry-date",
        "entry-number",
        "fact",
        "field",
        "resource",
    }
    missing_critical = critical_columns - output_columns
    assert (
        len(missing_critical) == 0
    ), f"Missing critical long-format columns: {missing_critical}"


# Test that every row has required non-null values
def test_all_rows_have_required_fields(transformed_resource):
    transformed_path = transformed_resource["transformed_path"]
    output_df = pd.read_csv(transformed_path)

    # Check entity column
    assert "entity" in output_df.columns, "Output must have 'entity' column"
    null_entities = output_df["entity"].isna().sum()
    assert null_entities == 0, f"{null_entities} rows are missing entity values"
    assert output_df["entity"].dtype in [
        "int64",
        "float64",
    ], "Entity column should be numeric"

    # Check fact column (unique identifier for each row)
    assert "fact" in output_df.columns, "Output must have 'fact' column"
    null_facts = output_df["fact"].isna().sum()
    assert null_facts == 0, f"{null_facts} rows are missing fact values"

    # Check that fact values are unique
    duplicate_facts = output_df["fact"].duplicated().sum()
    assert (
        duplicate_facts == 0
    ), f"{duplicate_facts} duplicate fact values found (should be unique)"

    # Check field column
    assert "field" in output_df.columns, "Output must have 'field' column"
    null_fields = output_df["field"].isna().sum()
    assert null_fields == 0, f"{null_fields} rows are missing field values"

    # Check priority column exists and has valid values
    if "priority" in output_df.columns:
        assert output_df["priority"].dtype in [
            "int64",
            "float64",
        ], "Priority should be numeric"


# Test that dates are properly formatted and present
def test_date_fields_valid(transformed_resource):
    transformed_path = transformed_resource["transformed_path"]
    output_df = pd.read_csv(transformed_path)

    # Check entry-date
    assert "entry-date" in output_df.columns, "Output must have 'entry-date' column"
    null_entry_dates = output_df["entry-date"].isna().sum()
    assert (
        null_entry_dates == 0
    ), f"{null_entry_dates} rows are missing entry-date values"

    # Verify date format (should be YYYY-MM-DD or similar)
    sample_entry_date = str(output_df["entry-date"].iloc[0])
    assert (
        len(sample_entry_date) >= 8
    ), f"entry-date format seems invalid: {sample_entry_date}"

    # Check start-date field rows (not all rows will have start-date values)
    if "start-date" in output_df.columns:
        start_date_rows = output_df[output_df["field"] == "start-date"]
        if len(start_date_rows) > 0:
            # For rows where field='start-date', the value column should have date values
            start_date_values = start_date_rows["value"].notna().sum()
            assert start_date_values > 0, "start-date field rows should have values"
