"""
These tests also aim to the run transform the way the async-request-backend repo uses the pipeline to process data,
the tests are ported over from what the original async run_pipeline tests did but adapted to use the digital-land-python pipeline code directly.
"""

import csv
import os
from pathlib import Path
import pandas as pd

from digital_land.check import duplicate_reference_check
from digital_land.organisation import Organisation
from digital_land.pipeline import Pipeline
from digital_land.specification import Specification


def create_test_input_csv(test_dirs, resource, rows):
    """Helper function to create test input CSV files."""
    input_path = os.path.join(test_dirs["collection_dir"], resource)
    if rows:
        write_csv_rows(input_path, rows)
    else:
        # Create empty file
        open(input_path, "w").close()
    return input_path


def write_csv_rows(file_path, rows):
    """Write rows to a CSV with headers derived from the first row."""
    fieldnames = list(rows[0].keys())
    with open(file_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_pipeline_transform(
    test_dirs, dataset, resource, input_path, output_path, disable_lookups=True
):
    # Set up paths and parameters used in transform
    endpoints = ["d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"]
    organisation = "test-org"
    organisation_path = os.path.join(test_dirs["cache_dir"], "organisation.csv")

    # Transform function requires a mocked lookup.csv and Organisation Class.
    lookup_rows = [
        {
            "prefix": dataset,
            "resource": "",
            "entry-number": "",
            "organisation": organisation,
            "reference": "ABC_0001",
            "entity": 1,
        },
        {
            "prefix": "article-4-direction",
            "resource": "",
            "entry-number": "",
            "organisation": organisation,
            "reference": "a4d2",
            "entity": 10,
        },
    ]
    write_csv_rows(os.path.join(test_dirs["pipeline_dir"], "lookup.csv"), lookup_rows)

    write_csv_rows(
        organisation_path, [{"organisation": "test-org", "name": "Test Org"}]
    )

    specification = Specification(test_dirs["specification_dir"])
    pipeline = Pipeline(test_dirs["pipeline_dir"], dataset, specification=specification)
    organisation = Organisation(organisation_path, Path(pipeline.path))

    issue_log = pipeline.transform(
        resource=resource,
        organisation=organisation,
        organisations=["local-authority:test-org"],  # Essentially a providers list
        endpoints=endpoints,
        input_path=input_path,
        output_path=output_path,
        valid_category_values={},
        disable_lookups=disable_lookups,
    )

    column_field_path = os.path.join(test_dirs["column_field_dir"], resource + ".csv")
    dataset_resource_path = os.path.join(
        test_dirs["dataset_resource_dir"], resource + ".csv"
    )

    # For purpose of check tool, duplicate reference check is run externally to transform
    issue_log = duplicate_reference_check(issues=issue_log, csv_path=output_path)
    issue_log.save(os.path.join(test_dirs["issues_log_dir"], resource + ".csv"))
    issue_log_path = os.path.join(test_dirs["issues_log_dir"], f"{resource}.csv")

    pipeline.save_logs(
        column_field_path=column_field_path,
        dataset_resource_path=dataset_resource_path,
    )

    return {
        "issue_log_path": issue_log_path,
        "column_field_path": column_field_path,
        "dataset_resource_path": dataset_resource_path,
    }


def test_async_pipeline_run(test_dirs):
    dataset = "national-park"
    resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"

    rows = [
        {
            "reference": "ABC_0001",
            "entry-date": "2024-01-01",
            "organisation": "test-org",
        },
        {
            "reference": "ABC_0002",
            "entry-date": "2024-01-02",
            "organisation": "test-org",
        },
    ]
    input_path = create_test_input_csv(test_dirs, resource, rows)
    output_path = Path(test_dirs["transformed_dir"]) / f"{resource}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Run the pipeline
    pipeline_logs = run_pipeline_transform(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        input_path=input_path,
        output_path=output_path,
    )

    for path, error_message in (
        (pipeline_logs["issue_log_path"], "Issue log file was not created"),
        (pipeline_logs["column_field_path"], "Column field log file was not created"),
        (
            pipeline_logs["dataset_resource_path"],
            "Dataset resource log file was not created",
        ),
    ):
        assert os.path.exists(path), error_message

    assert output_path.exists(), "Output file should be created"
    output_df = pd.read_csv(output_path)
    expected_columns = [
        "reference-entity",
        "entry-date",
        "entity",
        "start-date",
        "end-date",
    ]
    for col in expected_columns:
        assert col in output_df.columns, f"Missing expected column '{col}' in output."
    assert len(output_df) >= len(
        rows
    ), "Output row count does not match input row count."


def test_pipeline_output_is_complete(test_dirs):
    dataset = "national-park"
    resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    rows = [
        {
            "reference": "ABC_0001",
            "entry-date": "2024-01-01",
            "organisation": "test-org",
        },
        {
            "reference": "ABC_0002",
            "entry-date": "2024-01-02",
            "organisation": "test-org",
        },
    ]

    input_path = create_test_input_csv(test_dirs, resource, rows)
    output_path = Path(test_dirs["transformed_dir"]) / f"{resource}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    run_pipeline_transform(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        input_path=input_path,
        output_path=output_path,
    )
    assert output_path.exists(), "Output file was not created."
    output_df = pd.read_csv(output_path)
    assert not output_df.empty, "Output file is empty."


def test_pipeline_with_empty_input(test_dirs):
    dataset = "national-park"
    resource = "empty_input_test"

    input_path = create_test_input_csv(test_dirs, resource, [])
    output_path = Path(test_dirs["transformed_dir"]) / f"{resource}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    run_pipeline_transform(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        input_path=input_path,
        output_path=output_path,
    )
    assert output_path.exists(), "Output file was not created for empty input."
    output_df = pd.read_csv(output_path)
    assert output_df.empty, "Output file should be empty for empty input."


def test_issue_log_creation(test_dirs):
    dataset = "national-park"
    resource = "issue_log_test"
    rows = [
        {
            "reference": "issue_log_test1",
            "entry-date": "2023-12-12",
            "organisation": "test-org",
        },
        {
            "reference": "issue_log_test2",
            "entry-date": "2024-01-02",
            "organisation": "test-org",
        },
    ]

    input_path = create_test_input_csv(test_dirs, resource, rows)
    output_path = Path(test_dirs["transformed_dir"]) / f"{resource}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pipeline_logs = run_pipeline_transform(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        input_path=input_path,
        output_path=output_path,
    )
    issue_log_path = pipeline_logs["issue_log_path"]
    assert os.path.exists(issue_log_path), "Issue log was not created."

    # Load the issue log and check its contents
    issue_log_df = pd.read_csv(issue_log_path)
    assert not issue_log_df.empty, "Issue log should not be empty."
    assert (
        "issue-type" in issue_log_df.columns
    ), "Issue log is missing 'issue-type' column."


def test_column_field_log_creation(test_dirs):
    dataset = "national-park"
    resource = "column_field_log_test"
    rows = [
        {
            "reference": "ABC_0001",
            "entry-date": "2024-01-01",
            "organisation": "test-org",
        },
        {
            "reference": "ABC_0002",
            "entry-date": "2024-01-02",
            "organisation": "test-org",
        },
    ]

    input_path = create_test_input_csv(test_dirs, resource, rows)
    output_path = Path(test_dirs["transformed_dir"]) / f"{resource}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pipeline_logs = run_pipeline_transform(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        input_path=input_path,
        output_path=output_path,
    )

    # Check that the column field log was created
    column_field_log_path = pipeline_logs["column_field_path"]
    assert os.path.exists(column_field_log_path), "Column field log was not created."

    # Load the column field log and check its contents
    column_field_log_df = pd.read_csv(column_field_log_path)
    assert not column_field_log_df.empty, "Column field log should not be empty."
    assert (
        "field" in column_field_log_df.columns
    ), "Column field log is missing 'field' column."


def test_pipeline_lookup_phase(test_dirs):
    dataset = "article-4-direction-area"
    resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"

    rows = [
        {
            "reference": "ABC_0001",
            "entry-date": "2025-01-01",
            "organisation": "test-org",
            "article-4-direction": "a4d1",
        },
    ]

    input_path = create_test_input_csv(test_dirs, resource, rows)
    output_path = Path(test_dirs["transformed_dir"]) / f"{resource}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pipeline_logs = run_pipeline_transform(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        input_path=input_path,
        output_path=output_path,
        disable_lookups=False,
    )
    issue_log_path = pipeline_logs["issue_log_path"]
    issue_dict = next(
        (
            row
            for _, row in pd.read_csv(issue_log_path).iterrows()
            if row.get("field") == "article-4-direction"
        ),
        None,
    )

    assert os.path.exists(issue_log_path)
    assert "missing associated entity" == issue_dict["issue-type"]
    assert "a4d1" == issue_dict["value"]


def test_pipeline_lookup_phase_assign_reference_entity(test_dirs):
    dataset = "article-4-direction-area"
    resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"

    rows = [
        {
            "reference": "ABC_0001",
            "entry-date": "2025-01-01",
            "organisation": "test-org",
            "article-4-direction": "a4d2",
        },
    ]

    input_path = create_test_input_csv(test_dirs, resource, rows)
    output_path = Path(test_dirs["transformed_dir"]) / f"{resource}.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    pipeline_logs = run_pipeline_transform(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        input_path=input_path,
        output_path=output_path,
        disable_lookups=False,
    )
    issue_log_path = pipeline_logs["issue_log_path"]

    issue_log_df = pd.read_csv(issue_log_path)
    assert all(
        row.get("issue-type") != "missing associated entity"
        for _, row in issue_log_df.iterrows()
    )

    output_df = pd.read_csv(output_path)

    assert os.path.exists(output_path)
    assert 10.0 in output_df["reference-entity"].values
