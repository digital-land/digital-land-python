import pytest
import os
import csv
import urllib

from pathlib import Path
from digital_land.collection import Collection

from digital_land.commands import assign_entities, check_and_assign_entities
from digital_land.pipeline import Lookups


@pytest.fixture
def mock_resource():
    row1 = {
        "reference": "Ref1",
        "organisation": "government-organisation:D1342",
        "value": "test",
        "filter_type": "A",
    }
    row2 = {
        "reference": "Ref2",
        "organisation": "government-organisation:D1342",
        "value": "test",
        "filter_type": "A",
    }
    row3 = {
        "reference": "Ref3",
        "organisation": "government-organisation:D1342",
        "value": "test",
        "filter_type": "B",
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
        "datasets": "tree;tree-preservation-zone",
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
    row = {"endpoint": "endpoint", "pipelines": "tree;tree-preservation-zone"}
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
        "provision-rule.csv",
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

    fieldnames_filter = {
        "dataset",
        "resource",
        "field",
        "pattern",
        "entry-number",
        "start-date",
        "end-date",
        "entry-date",
        "endpoint",
    }

    rows = [
        {
            "dataset": "tree-preservation-zone",
            "resource": "mock_csv",
            "field": "filter_type",
            "pattern": "A",
            "entry-number": "",
            "start-date": "",
            "end-date": "",
            "entry-date": "",
            "endpoint": "endpoint",
        },
        {
            "dataset": "tree",
            "resource": "mock_csv",
            "field": "filter_type",
            "pattern": "B",
            "entry-number": "",
            "start-date": "",
            "end-date": "",
            "entry-date": "",
            "endpoint": "endpoint",
        },
    ]
    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()

    with open(os.path.join(pipeline_dir, "filter.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames_filter)
        dictwriter.writeheader()
        for row in rows:
            dictwriter.writerow(row)

    return pipeline_dir


def test_command_assign_entities(
    capfd,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    mock_resource,
):
    """
    This tests a scenario if filter was provided on a column in data and
    not on mapped field (or a field in specification), it should still
    consider it and filter the rows accordingly.
    """
    collection_name = "tree-preservation-order"
    test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"

    collection = Collection(name=collection_name, directory=collection_dir)
    collection.load()
    dataset_resource_map = collection.dataset_resource_map()
    for dataset in dataset_resource_map:
        assign_entities(
            resource_file_paths=["mock_csv.csv"],
            collection=collection,
            organisation=["government-organisation:D1342"],
            specification_dir=specification_dir,
            organisation_path=organisation_path,
            pipeline_dir=pipeline_dir,
            dataset=dataset,
            endpoints=test_endpoint,
        )

    lookups = Lookups(pipeline_dir)
    lookups.load_csv()

    out, err = capfd.readouterr()

    assert (
        "tree-preservation-zone , government-organisation:D1342 , Ref1 , 19100000"
        in out
    )
    assert (
        "tree-preservation-zone , government-organisation:D1342 , Ref2 , 19100001"
        in out
    )
    assert "tree , government-organisation:D1342 , Ref3 , 7002000000" in out


def test_check_and_assign_entities(
    capfd,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    mock_resource,
):
    """
    Test verifies transformed file is created with the new entities.
    """
    collection_name = "tree-preservation-order"
    test_endpoint = "endpoint"

    check_and_assign_entities(
        resource_file_paths=[mock_resource],
        endpoints=[test_endpoint],
        collection_name=collection_name,
        dataset=collection_name,
        organisation=["government-organisation:D1342"],
        collection_dir=collection_dir,
        organisation_path=organisation_path,
        specification_dir=specification_dir,
        pipeline_dir=pipeline_dir,
    )
    out, err = capfd.readouterr()
    assert "Total number of new entities: 3" in out

    resource = os.path.basename(mock_resource)
    output_path = Path("var/cache/assign_entities/transformed") / f"{resource}.csv"

    assert output_path.exists(), "Expected transformed file not found."

    with open(output_path, "r", encoding="utf-8") as f:
        content = f.read()

    assert "reference,2,,mock_csv,,Ref1" in content
    assert "reference,2,,mock_csv,,Ref2" in content
    assert "reference,2,,mock_csv,,Ref3" in content
