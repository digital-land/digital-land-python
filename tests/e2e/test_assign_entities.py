import pytest
import os
import csv
import urllib

from pathlib import Path
from click.testing import CliRunner

from digital_land.commands import assign_entities
from digital_land.pipeline import Lookups
from digital_land.cli import assign_entities_cmd
from digital_land.specification import Specification


@pytest.fixture
def mock_resource():
    row1 = {
        "reference": "Ref1",
        "organisation": "government-organisation:D1342",
        "value": "test",
    }
    row2 = {
        "reference": "Ref2",
        "organisation": "government-organisation:D1342",
        "value": "test",
    }
    row3 = {
        "reference": "",
        "organisation": "government-organisation:D1342",
        "value": "test",
    }

    mock_csv_path = Path("mock_csv.csv")
    with open(mock_csv_path, "w", encoding="utf-8") as f:
        dictwriter = csv.DictWriter(f, fieldnames=row1.keys())
        dictwriter.writeheader()
        dictwriter.writerow(row1)
        dictwriter.writerow(row2)
        dictwriter.writerow(row3)

    yield mock_csv_path

    os.remove(mock_csv_path)


@pytest.fixture
def collection_dir(tmp_path):
    collection_dir = os.path.join(tmp_path, "collection")
    os.makedirs(collection_dir, exist_ok=True)

    # create source
    source_fieldnames = [
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "licence",
        "organisation",
        "pipelines",
        "entry-date",
        "start-date",
        "end-date",
    ]

    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=source_fieldnames)
        dictwriter.writeheader()

    endpoint_fieldnames = [
        "endpoint",
        "endpoint-url",
        "parameters",
        "plugin",
        "entry-date",
        "start-date",
        "end-date",
    ]
    with open(os.path.join(collection_dir, "endpoint.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=endpoint_fieldnames)
        dictwriter.writeheader()

    resource_fieldnames = [
        "resource",
        "bytes",
        "organisations",
        "datasets",
        "endpoints",
        "start-date",
        "end-date",
    ]
    row = {
        "resource": "mock_csv",
        "datasets": "anciend-woodland",
        "endpoints": "endpoint",
    }
    with open(os.path.join(collection_dir, "resource.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=resource_fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    log_fieldnames = [
        "bytes",
        "content-type",
        "elapsed",
        "endpoint",
        "resource",
        "status",
        "entry-date",
        "start-date",
        "end-date",
        "exception",
    ]
    with open(os.path.join(collection_dir, "log.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=log_fieldnames)
        dictwriter.writeheader()

    source_fieldnames = [
        "source",
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "licence",
        "organisation",
        "pipelines",
        "entry-date",
        "start-date",
        "end-date",
    ]
    row = {"endpoint": "endpoint", "pipelines": "ancient-woodland"}
    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=source_fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    return collection_dir


@pytest.fixture(scope="session")
def specification_dir(tmp_path_factory):
    specification_dir = tmp_path_factory.mktemp("specification")
    source_url = "https://raw.githubusercontent.com/digital-land/"
    specification_csvs = [
        "attribution.csv",
        "licence.csv",
        "typology.csv",
        "theme.csv",
        "collection.csv",
        "dataset.csv",
        "dataset-field.csv",
        "field.csv",
        "datatype.csv",
        "prefix.csv",
        # deprecated ..
        "pipeline.csv",
        "dataset-schema.csv",
        "schema.csv",
        "schema-field.csv",
    ]
    for specification_csv in specification_csvs:
        urllib.request.urlretrieve(
            f"{source_url}/specification/main/specification/{specification_csv}",
            os.path.join(specification_dir, specification_csv),
        )

    return specification_dir


@pytest.fixture
def organisation_csv(tmp_path):
    organisation_path = os.path.join(tmp_path, "organisation.csv")
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv",
        organisation_path,
    )
    return organisation_path


@pytest.fixture
def pipeline_dir(tmp_path):
    pipeline_dir = os.path.join(tmp_path, "pipeline")
    os.makedirs(pipeline_dir, exist_ok=True)

    fieldnames = {
        "prefix",
        "resource",
        "entry-number",
        "organisation",
        "reference",
        "entity",
    }

    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()

    return pipeline_dir


def test_command_assign_entities(
    collection_dir, pipeline_dir, specification_dir, organisation_path, mock_resource
):

    """
    This tests a function that, given a specific resource hash for an endpoint already added to the system,
    should identify any missing lookups and add them to lookup.csv
    """
    collection_name = "ancient-woodland"
    dataset_name = "ancient-woodland"

    assign_entities(
        resource_file_paths=["mock_csv.csv"],
        collection_name=collection_name,
        collection_dir=collection_dir,
        specification_dir=specification_dir,
        organisation_path=organisation_path,
        pipeline_dir=pipeline_dir,
    )

    lookups = Lookups(pipeline_dir)
    lookups.load_csv()

    specification = Specification(specification_dir)
    entity_range_min = specification.get_dataset_entity_min(dataset_name)
    entity_range_max = specification.get_dataset_entity_max(dataset_name)

    assert len(lookups.entries) > 0
    assert lookups.entries[0]["entity"] == entity_range_min
    assert lookups.entries[0]["reference"] == "Ref1"
    assert lookups.entries[0]["prefix"] == dataset_name
    assert lookups.entries[1]["entity"] == str(int(entity_range_min) + 1)
    assert lookups.entries[1]["reference"] == "Ref2"
    assert lookups.entries[1]["prefix"] == dataset_name

    entity_numbers = []
    for entry in lookups.entries:
        entity_numbers.append(entry.get("entity"))
    assert min(int(entity) for entity in entity_numbers) >= int(entity_range_min)
    assert max(int(entity) for entity in entity_numbers) <= int(entity_range_max)


def test_cli_assign_entities_success(
    collection_dir, pipeline_dir, specification_dir, organisation_path, mock_resource
):

    """
    Tests assign entities from cli
    """

    collection_name = "ancient-woodland"
    resource_path = "mock_csv.csv"

    runner = CliRunner()
    result = runner.invoke(
        assign_entities_cmd,
        [
            resource_path,
            collection_name,
            # these will be optional to the user but included here to point at files stored elsewhere
            "--collection-dir",
            collection_dir,
            "--pipeline-dir",
            pipeline_dir,
            "--specification-dir",
            specification_dir,
            "--organisation-path",
            organisation_path,
        ],
    )

    assert result.exit_code == 0, f"{result.stdout}"


def test_cli_assign_entities_failure_resource_not_found(
    collection_dir, pipeline_dir, specification_dir, organisation_path, mock_resource
):

    """
    Tests assign entities from cli when resource path is incorrect
    """

    collection_name = "ancient-woodland"
    resource_path = ""

    runner = CliRunner()
    result = runner.invoke(
        assign_entities_cmd,
        [
            resource_path,
            collection_name,
            # these will be optional to the user but included here to point at files stored elsewhere
            "--collection-dir",
            collection_dir,
            "--pipeline-dir",
            pipeline_dir,
            "--specification-dir",
            specification_dir,
            "--organisation-path",
            organisation_path,
        ],
    )

    assert result.exit_code == 2, f"{result.stdout}"


def test_command_assign_entities_no_reference_log(
    caplog,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    mock_resource,
):

    """
    This tests that the assign entities command logs a warning when there is an entry
    with no reference
    """
    collection_name = "ancient-woodland"

    assign_entities(
        resource_file_paths=["mock_csv.csv"],
        collection_name=collection_name,
        collection_dir=collection_dir,
        specification_dir=specification_dir,
        organisation_path=organisation_path,
        pipeline_dir=pipeline_dir,
    )

    assert "No reference" in caplog.text
    assert "mock_csv" in caplog.text