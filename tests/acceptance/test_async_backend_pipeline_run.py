import csv
import os
from pathlib import Path
import pandas as pd
from digital_land.check import duplicate_reference_check
from digital_land.commands import default_output_path
from digital_land.log import ColumnFieldLog, DatasetResourceLog, IssueLog
from digital_land.organisation import Organisation
from digital_land.phase.combine import FactCombinePhase
from digital_land.phase.concat import ConcatFieldPhase
from digital_land.phase.convert import ConvertPhase
from digital_land.phase.default import DefaultPhase
from digital_land.phase.factor import FactorPhase
from digital_land.phase.filter import FilterPhase
from digital_land.phase.harmonise import HarmonisePhase
from digital_land.phase.lookup import EntityLookupPhase, FactLookupPhase
from digital_land.phase.map import MapPhase
from digital_land.phase.migrate import MigratePhase
from digital_land.phase.normalise import NormalisePhase
from digital_land.phase.organisation import OrganisationPhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.patch import PatchPhase
from digital_land.phase.pivot import PivotPhase
from digital_land.phase.prefix import EntityPrefixPhase
from digital_land.phase.priority import PriorityPhase
from digital_land.phase.prune import FactPrunePhase, FieldPrunePhase
from digital_land.phase.reference import EntityReferencePhase, FactReferencePhase
from digital_land.phase.save import SavePhase
from digital_land.pipeline import Pipeline, run_pipeline
from digital_land.specification import Specification

# These tests aims to emulate the way the async-request-backend repo uses the pipeline to process
# If these tests are breaking it likely means your changes will also break the request-processor in async-backend


def run_pipeline_for_test(test_dirs, dataset, resource, request_id, input_path):
    endpoints = ["d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"]
    pipeline_dir = test_dirs["pipeline_dir"]
    organisation = "test-org:test"
    request_id = request_id
    collection_dir = test_dirs["collection_dir"]
    converted_dir = test_dirs["converted_resource_dir"]
    issue_dir = test_dirs["issues_log_dir"]
    column_field_dir = test_dirs["column_field_dir"]
    transformed_dir = test_dirs["transformed_dir"]
    dataset_resource_dir = test_dirs["dataset_resource_dir"]
    specification_dir = test_dirs["specification_dir"]
    cache_dir = test_dirs["cache_dir"]

    organisation_path = os.path.join(cache_dir, "organisation.csv")
    input_path = os.path.join(collection_dir, resource)
    output_path = os.path.join(transformed_dir, dataset, request_id, f"{resource}.csv")
    save_harmonised = False

    # Create lookup.csv file with data
    row = {
        "prefix": dataset,
        "resource": "",
        "entry-number": "",
        "organisation": organisation,
        "reference": "ABC_0001",
        "entity": 1,
    }
    row2 = {
        "prefix": "article-4-direction",
        "resource": "",
        "entry-number": "",
        "organisation": organisation,
        "reference": "a4d2",
        "entity": 10,
    }

    fieldnames = row.keys()
    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)
        dictwriter.writerow(row2)

    # Create organisation.csv with data
    row = {"organisation": "test-org:test", "name": "Test Org"}
    fieldnames = row.keys()
    with open(organisation_path, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    # Create resource with data

    pipeline = Pipeline(pipeline_dir, dataset)
    specification = Specification(specification_dir)

    # Ensure all directories for this 'request' are made
    os.makedirs(os.path.join(issue_dir, dataset, request_id), exist_ok=True)
    os.makedirs(os.path.join(transformed_dir, dataset, request_id), exist_ok=True)
    os.makedirs(os.path.join(column_field_dir, dataset, request_id), exist_ok=True)
    os.makedirs(os.path.join(dataset_resource_dir, dataset, request_id), exist_ok=True)

    resource = Path(input_path).stem
    schema = specification.pipeline[pipeline.name]["schema"]
    intermediate_fieldnames = specification.intermediate_fieldnames(pipeline)
    issue_log = IssueLog(dataset=dataset, resource=resource)
    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)
    dataset_resource_log = DatasetResourceLog(dataset=dataset, resource=resource)

    # Load pipeline configuration
    skip_patterns = pipeline.skip_patterns(resource)
    columns = pipeline.columns(resource, endpoints=endpoints)
    concats = pipeline.concatenations(resource, endpoints=endpoints)
    patches = pipeline.patches(resource=resource)
    lookups = pipeline.lookups(resource=resource)
    default_fields = pipeline.default_fields(resource=resource)
    default_values = pipeline.default_values(endpoints=endpoints)
    combine_fields = pipeline.combine_fields(endpoints=endpoints)

    # Load organisations
    organisation = Organisation(organisation_path, Path(pipeline.path))
    default_values["organisation"] = "test-org:test"
    try:
        run_pipeline(
            ConvertPhase(
                path=input_path,
                dataset_resource_log=dataset_resource_log,
                output_path=os.path.join(converted_dir, request_id, f"{resource}.csv"),
            ),
            NormalisePhase(skip_patterns=skip_patterns),
            ParsePhase(),
            ConcatFieldPhase(concats=concats, log=column_field_log),
            MapPhase(
                fieldnames=intermediate_fieldnames,
                columns=columns,
                log=column_field_log,
            ),
            FilterPhase(filters=pipeline.filters(resource)),
            PatchPhase(
                issues=issue_log,
                patches=patches,
            ),
            HarmonisePhase(
                field_datatype_map=specification.get_field_datatype_map(),
                issues=issue_log,
                dataset=dataset,
            ),
            DefaultPhase(
                default_fields=default_fields,
                default_values=default_values,
                issues=issue_log,
            ),
            # TBD: move migrating columns to fields to be immediately after map
            # this will simplify harmonisation and remove intermediate_fieldnames
            # but effects brownfield-land and other pipelines which operate on columns
            MigratePhase(
                fields=specification.schema_field[schema],
                migrations=pipeline.migrations(),
            ),
            OrganisationPhase(organisation=organisation, issues=issue_log),
            FieldPrunePhase(fields=specification.current_fieldnames(schema)),
            EntityReferencePhase(
                dataset=dataset,
                prefix=specification.dataset_prefix(dataset),
            ),
            EntityPrefixPhase(dataset=dataset),
            EntityLookupPhase(lookups),
            SavePhase(
                default_output_path("harmonised", input_path),
                fieldnames=intermediate_fieldnames,
                enabled=save_harmonised,
            ),
            PriorityPhase(config=None),
            PivotPhase(),
            FactCombinePhase(issue_log=issue_log, fields=combine_fields),
            FactorPhase(),
            FactReferencePhase(
                field_typology_map=specification.get_field_typology_map(),
                field_prefix_map=specification.get_field_prefix_map(),
            ),
            FactLookupPhase(
                lookups,
                issue_log=issue_log,
                odp_collections=specification.get_odp_collections(),
            ),
            FactPrunePhase(),
            SavePhase(
                output_path,
                fieldnames=specification.factor_fieldnames(),
            ),
        )
    except Exception as e:
        print("Pipeline failed during execution:", e)

    issue_log = duplicate_reference_check(issues=issue_log, csv_path=output_path)
    issue_log.save(os.path.join(issue_dir, resource + ".csv"))
    issue_log_path = os.path.join(issue_dir, f"{resource}.csv")
    column_field_log.save(os.path.join(column_field_dir, resource + ".csv"))
    column_field_log_path = os.path.join(column_field_dir, f"{resource}.csv")
    dataset_resource_log.save(os.path.join(dataset_resource_dir, resource + ".csv"))
    dataset_resource_log_path = os.path.join(
        test_dirs["dataset_resource_dir"], resource + ".csv"
    )

    return {
        "output_path": output_path,
        "issue_log": issue_log_path,
        "save_issue_log": issue_log,
        "column_field_log": column_field_log_path,
        "dataset_resource_log": dataset_resource_log_path,
    }


def test_async_pipeline_run(test_dirs):
    dataset = "national-park"
    resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    request_id = "test_request_id"

    rows = [
        {
            "reference": "ABC_0001",
            "entry-date": "2024-01-01",
            "organisation": "test-org:test",
        },
        {
            "reference": "ABC_0002",
            "entry-date": "2024-01-02",
            "organisation": "test-org:test",
        },
    ]

    input_path = os.path.join(test_dirs["collection_dir"], resource)
    with open(input_path, "w", newline="") as f:
        fieldnames = ["reference", "entry-date", "organisation"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Run the pipeline
    run_pipeline = run_pipeline_for_test(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        request_id=request_id,
        input_path=input_path,
    )
    # issue_log
    issue_log_path = run_pipeline.get("issue_log")
    assert os.path.exists(issue_log_path), "Issue log file was not created"

    # column_log
    column_field_log_path = run_pipeline.get("column_field_log")
    assert os.path.exists(
        column_field_log_path
    ), "Column field log file was not created"

    # dataset_resource_log
    dataset_resource_log_path = run_pipeline.get("dataset_resource_log")
    assert os.path.exists(
        dataset_resource_log_path
    ), "Dataset resource log file was not created"

    output_path = run_pipeline.get("output_path")
    assert os.path.exists(output_path), "Pipeline failed to generate output file."
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
    request_id = "test_request_id"
    rows = [
        {
            "reference": "ABC_0001",
            "entry-date": "2024-01-01",
            "organisation": "test-org:test",
        },
        {
            "reference": "ABC_0002",
            "entry-date": "2024-01-02",
            "organisation": "test-org:test",
        },
    ]

    input_path = os.path.join(test_dirs["collection_dir"], resource)
    with open(input_path, "w", newline="") as f:
        fieldnames = ["reference", "entry-date", "organisation"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Run the pipeline
    run_pipeline = run_pipeline_for_test(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        request_id=request_id,
        input_path=input_path,
    )
    output_path = run_pipeline.get("output_path")
    assert os.path.exists(output_path), "Output file was not created."
    output_df = pd.read_csv(output_path)
    assert not output_df.empty, "Output file is empty."


def test_pipeline_with_empty_input(test_dirs):
    dataset = "national-park"
    resource = "empty_input_test"
    request_id = "empty_test"

    input_path = os.path.join(test_dirs["collection_dir"], resource)
    # Create an empty input file
    open(input_path, "a").close()

    run_pipeline = run_pipeline_for_test(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        request_id=request_id,
        input_path=input_path,
    )
    output_path = run_pipeline.get("output_path")
    assert os.path.exists(output_path), "Output file was not created for empty input."
    output_df = pd.read_csv(output_path)
    assert output_df.empty, "Output file should be empty for empty input."


def test_issue_log_creation(test_dirs):
    dataset = "national-park"
    resource = "issue_log_test"
    request_id = "issue_log_test_id"
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

    input_path = os.path.join(test_dirs["collection_dir"], resource)
    with open(input_path, "w", newline="") as f:
        fieldnames = ["reference", "entry-date", "organisation"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Run the pipeline
    run_pipeline = run_pipeline_for_test(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        request_id=request_id,
        input_path=input_path,
    )
    issue_log_path = run_pipeline.get("issue_log")
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
    request_id = "column_field_log_test_id"
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

    input_path = os.path.join(test_dirs["collection_dir"], resource)
    with open(input_path, "w", newline="") as f:
        fieldnames = ["reference", "entry-date", "organisation"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    run_pipeline = run_pipeline_for_test(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        request_id=request_id,
        input_path=input_path,
    )

    # Check that the column field log was created
    column_field_log_path = run_pipeline.get("column_field_log")
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
    request_id = "test_request_id"

    rows = [
        {
            "reference": "ABC_0001",
            "entry-date": "2025-01-01",
            "organisation": "test-org:test",
            "article-4-direction": "a4d1",
        }
    ]

    input_path = os.path.join(test_dirs["collection_dir"], resource)

    with open(input_path, "w", newline="") as f:
        fieldnames = ["reference", "entry-date", "organisation", "article-4-direction"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Run the pipeline
    run_pipeline = run_pipeline_for_test(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        request_id=request_id,
        input_path=input_path,
    )
    # issue_log
    issue_log = run_pipeline.get("save_issue_log")
    issue_log_path = run_pipeline.get("issue_log")
    issue_dict = next(
        (
            issue
            for issue in issue_log.rows
            if issue.get("field") == "article-4-direction"
        ),
        None,
    )

    assert os.path.exists(issue_log_path)
    assert "missing associated entity" == issue_dict["issue-type"]
    assert "a4d1" == issue_dict["value"]


def test_pipeline_lookup_phase_assign_reference_entity(test_dirs):
    dataset = "article-4-direction-area"
    resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    request_id = "test_request_id"

    rows = [
        {
            "reference": "ABC_0001",
            "entry-date": "2025-01-01",
            "organisation": "test-org:test",
            "article-4-direction": "a4d2",
        }
    ]

    input_path = os.path.join(test_dirs["collection_dir"], resource)

    with open(input_path, "w", newline="") as f:
        fieldnames = ["reference", "entry-date", "organisation", "article-4-direction"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Run the pipeline
    run_pipeline = run_pipeline_for_test(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        request_id=request_id,
        input_path=input_path,
    )
    # issue_log
    issue_log = run_pipeline.get("save_issue_log")

    # assert given error does not exist in issue_log
    assert all(
        issue.get("issue-type") != "missing associated entity"
        for issue in issue_log.rows
    )

    output_path = run_pipeline.get("output_path")
    output_df = pd.read_csv(output_path)

    assert os.path.exists(output_path)
    assert 10.0 in output_df["reference-entity"].values
