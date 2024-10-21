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

def run_pipeline_for_test(
    test_dirs, dataset, resource, request_id, input_path
):

    endpoints = ["d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"]
    pipeline_dir = test_dirs["pipeline_dir"]
    organisation = "test-org"
    request_id = request_id
    collection_dir = test_dirs["collection_dir"]
    converted_dir = test_dirs["converted_resource_dir"]
    issue_dir = test_dirs["issues_log_dir"]
    column_field_dir = test_dirs["column_field_dir"]
    transformed_dir = test_dirs["transformed_dir"]
    dataset_resource_dir = test_dirs["dataset_resource_dir"]
    specification_dir = test_dirs["specification_dir"]
    cache_dir = test_dirs["cache_dir"]
    custom_temp_dir = test_dirs["custom_temp_dir"]

    organisation_path = os.path.join(cache_dir, "organisation.csv")
    input_path = os.path.join(collection_dir, resource)
    null_path = None
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
    fieldnames = row.keys()
    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    # Create organisation.csv with data
    row = {"organisation": "test-org", "name": "Test Org"}
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

    input_df = pd.read_csv(input_path)
    if input_df.empty:
        print("Input DataFrame is empty. Check if the input CSV is created correctly.")
        return None

    input_path = os.path.join(collection_dir, "resource", request_id)
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
    severity_csv_path = os.path.join(specification_dir, "issue-type.csv")
    default_values["organisation"] = organisation
    try:
        run_pipeline(
            ConvertPhase(
                path=input_path,
                dataset_resource_log=dataset_resource_log,
                custom_temp_dir=custom_temp_dir,
                output_path=os.path.join(converted_dir, request_id, f"{resource}.csv"),
            ),
            NormalisePhase(skip_patterns=skip_patterns, null_path=null_path),
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
            FactLookupPhase(lookups),
            FactPrunePhase(),
            SavePhase(
                output_path,
                fieldnames=specification.factor_fieldnames(),
            ),
        )
    except Exception as e:
        # Handle and log specific exceptions
        print("Pipeline failed during execution:", e)

    issue_log = duplicate_reference_check(issues=issue_log, csv_path=output_path)

    # Add the 'severity' and 'description' column based on the mapping
    issue_log.add_severity_column(severity_csv_path)

    issue_log.save(os.path.join(issue_dir, resource + ".csv"))
    column_field_log.save(os.path.join(column_field_dir, resource + ".csv"))
    dataset_resource_log.save(os.path.join(dataset_resource_dir, resource + ".csv"))

    return output_path


def test_pipeline_output_is_complete(test_dirs):
    dataset = "national-park"
    resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
    request_id = "test_request_id"

    # Prepare input CSV file
    rows = [
        {"reference": "ABC_0001", "entry-date": "2024-01-01"},
        {"reference": "ABC_0002", "entry-date": "2024-01-02"},
    ]
    input_path = os.path.join(test_dirs["collection_dir"], resource)
    with open(input_path, "w", newline='') as f:
        fieldnames = ["reference", "entry-date"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Run the pipeline
    output_path = run_pipeline_for_test(
        test_dirs=test_dirs,
        dataset=dataset,
        resource=resource,
        request_id=request_id,
        input_path=input_path
    )
    input_df = pd.read_csv(input_path)
    print("Input DataFrame:\n", input_df)
    assert os.path.exists(output_path), "Output file was not created."

    output_df = pd.read_csv(output_path)
    print("Output DataFrame:\n", output_df)
    assert not output_df.empty, "Output file is empty."
