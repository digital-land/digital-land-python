import csv
from datetime import datetime
import os
import shutil
import tempfile
from unittest.mock import Mock
import pandas as pd
import pytest

from digital_land.collection import Collection
from digital_land.specification import Specification
from digital_land.utils.add_data_utils import (
    clear_log,
    download_dataset,
    get_column_field_summary,
    get_entity_summary,
    get_existing_endpoints_summary,
    get_issue_summary,
    get_transformed_entities,
    get_updated_entities_summary,
)


def test_clear_logs(tmp_path_factory):
    today = datetime.utcnow().isoformat()[:10]
    endpoint = "endpoint"
    collection_dir = tmp_path_factory.mktemp("random_collection")

    file_path = os.path.join(collection_dir, "log", today, f"{endpoint}.json")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        f.write("hello")

    clear_log(collection_dir, endpoint)

    assert not os.path.isfile(file_path)


def test_get_issue_summary(tmp_path_factory):
    issue_dir = tmp_path_factory.mktemp("issue")

    resource = "resource"
    endpoint_resource_info = {"resource": resource}

    headers = ["issue-type", "field", "value"]
    rows = [
        {"issue-type": "issue-type1", "field": "field1", "value": "issue1"},
        {"issue-type": "issue-type1", "field": "field2", "value": "issue2"},
        {"issue-type": "issue-type2", "field": "field1", "value": "issue3"},
        {"issue-type": "issue-type2", "field": "field1", "value": "issue4"},
    ]
    with open(os.path.join(issue_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    issue_summary = get_issue_summary(endpoint_resource_info, issue_dir)

    assert "issue-type1  field1    1\n             field2    1" in issue_summary
    assert "issue-type2  field1    2" in issue_summary


def test_get_issue_summary_no_issues(tmp_path_factory):
    issue_dir = tmp_path_factory.mktemp("issue")

    resource = "resource"
    endpoint_resource_info = {"resource": resource}

    headers = ["issue-type", "field", "value"]

    with open(os.path.join(issue_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

    issue_summary = get_issue_summary(endpoint_resource_info, issue_dir)

    assert "No issues" in issue_summary


def test_get_issue_summary_new_entities(tmp_path_factory):
    issue_dir = tmp_path_factory.mktemp("issue")

    resource = "resource"
    endpoint_resource_info = {"resource": resource}

    headers = ["entity", "issue-type", "field", "value"]
    rows = [
        {
            "entity": 1,
            "issue-type": "issue-type1",
            "field": "field1",
            "value": "issue1",
        },
        {
            "entity": 1,
            "issue-type": "issue-type1",
            "field": "field2",
            "value": "issue2",
        },
        {
            "entity": 2,
            "issue-type": "issue-type2",
            "field": "field1",
            "value": "issue3",
        },
        {
            "entity": 2,
            "issue-type": "issue-type2",
            "field": "field1",
            "value": "issue4",
        },
        {
            "entity": 3,
            "issue-type": "issue-type3",
            "field": "field3",
            "value": "issue5",
        },
    ]
    with open(os.path.join(issue_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

    filtered_issue_summary = get_issue_summary(
        endpoint_resource_info, issue_dir, new_entities=[1, 3]
    )

    assert (
        "issue-type1  field1    1\n             field2    1" in filtered_issue_summary
    )
    assert "issue-type3  field3    1" in filtered_issue_summary
    assert "issue-type2" not in filtered_issue_summary


def test_get_entity_summary(tmp_path_factory):
    issue_dir = tmp_path_factory.mktemp("issue")
    transformed_dir = tmp_path_factory.mktemp("tranformed")
    pipeline_dir = tmp_path_factory.mktemp("pipeline")

    resource = "resource"
    endpoint_resource_info = {
        "organisation": "local-authority-eng:SST",
        "resource": resource,
    }
    pipeline = "dataset"

    issue_headers = ["issue-type", "field", "value", "line-number"]
    issue_rows = [
        {
            "issue-type": "unknown entity",
            "field": "field1",
            "value": "dataset:reference",
            "line-number": 1,
        },
        {
            "issue-type": "unknown entity - missing reference",
            "field": "field1",
            "value": "dataset:",
            "line-number": 2,
        },
        {"issue-type": "known entity", "field": "field1", "value": "n/a"},
    ]
    with open(os.path.join(issue_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=issue_headers)
        writer.writeheader()
        writer.writerows(issue_rows)

    output_path = os.path.join(transformed_dir, resource + ".csv")
    transformed_headers = ["entity"]
    transformed_rows = [
        {"entity": 1},
        {"entity": 1},
        {"entity": 1},
        {"entity": 2},
        {"entity": 3},
        {"entity": 4},
        {"entity": 4},
    ]
    with open(output_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=transformed_headers)
        writer.writeheader()
        writer.writerows(transformed_rows)

    rows = [
        {
            "prefix": "dataset",
            "resource": "",
            "entry-number": "",
            "organisation": "local-authority:SST",
            "reference": "reference",
            "entity": 10,
        },
        {
            "prefix": "dataset",
            "resource": "",
            "entry-number": "",
            "organisation": "local-authority:SST",
            "reference": "reference",
            "entity": 11,
        },
    ]

    fieldnames = rows[0].keys()

    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerows(rows)

    entity_summary = get_entity_summary(
        endpoint_resource_info, output_path, pipeline, issue_dir, pipeline_dir
    )
    assert "Number of existing entities in resource: 4" in entity_summary
    assert "Number of new entities in resource: 2" in entity_summary
    assert "reference  line-number" in entity_summary
    assert "reference            1" in entity_summary
    assert "NaN            2" in entity_summary


def test_get_entity_summary_missing_entity(tmp_path_factory):
    issue_dir = tmp_path_factory.mktemp("issue")
    transformed_dir = tmp_path_factory.mktemp("tranformed")
    pipeline_dir = tmp_path_factory.mktemp("pipeline")

    resource = "resource"
    endpoint_resource_info = {
        "organisation": "local-authority:SST",
        "resource": resource,
    }
    pipeline = "dataset"

    issue_headers = ["issue-type", "field", "value", "line-number"]
    with open(os.path.join(issue_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=issue_headers)
        writer.writeheader()

    output_path = os.path.join(transformed_dir, resource + ".csv")
    transformed_headers = ["entity"]
    transformed_rows = [
        {"entity": 1},
        {"entity": 2},
    ]
    with open(output_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=transformed_headers)
        writer.writeheader()
        writer.writerows(transformed_rows)

        # create lookups
    rows = [
        {
            "prefix": "dataset",
            "resource": "",
            "entry-number": "",
            "organisation": "local-authority:SST",
            "reference": "reference",
            "entity": 1,
        },
        {
            "prefix": "dataset",
            "resource": "",
            "entry-number": "",
            "organisation": "local-authority:SST",
            "reference": "reference2",
            "entity": 2,
        },
        {
            "prefix": "dataset",
            "resource": "",
            "entry-number": "",
            "organisation": "local-authority:SST",
            "reference": "reference3",
            "entity": 3,
        },
    ]

    fieldnames = rows[0].keys()

    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerows(rows)

    entity_summary = get_entity_summary(
        endpoint_resource_info, output_path, pipeline, issue_dir, pipeline_dir
    )
    assert (
        "WARNING: There are 1 entities on the platform for this provision that aren't present in this resource"
        in entity_summary
    )


def test_get_entity_summary_missing_all_entity(tmp_path_factory):
    issue_dir = tmp_path_factory.mktemp("issue")
    transformed_dir = tmp_path_factory.mktemp("tranformed")
    pipeline_dir = tmp_path_factory.mktemp("pipeline")

    resource = "resource"
    endpoint_resource_info = {
        "organisation": "local-authority:SST",
        "resource": resource,
    }
    pipeline = "dataset"

    issue_headers = ["issue-type", "field", "value", "line-number"]
    with open(os.path.join(issue_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=issue_headers)
        writer.writeheader()

    output_path = os.path.join(transformed_dir, resource + ".csv")
    transformed_headers = ["entity"]
    transformed_rows = [
        {"entity": 1},
        {"entity": 2},
    ]
    with open(output_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=transformed_headers)
        writer.writeheader()
        writer.writerows(transformed_rows)

        # create lookups
    rows = [
        {
            "prefix": "dataset",
            "resource": "",
            "entry-number": "",
            "organisation": "local-authority:SST",
            "reference": "reference",
            "entity": 11,
        },
        {
            "prefix": "dataset",
            "resource": "",
            "entry-number": "",
            "organisation": "local-authority:SST",
            "reference": "reference2",
            "entity": 12,
        },
        {
            "prefix": "dataset",
            "resource": "",
            "entry-number": "",
            "organisation": "local-authority:SST",
            "reference": "reference3",
            "entity": 13,
        },
    ]

    fieldnames = rows[0].keys()

    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerows(rows)

    entity_summary = get_entity_summary(
        endpoint_resource_info, output_path, pipeline, issue_dir, pipeline_dir
    )
    assert (
        "WARNING: NONE of the 3 entities on the platform for this provision are in the resource - is this correct?"
        in entity_summary
    )


# This test also tests the 'exclude fields' functionality
# as some of those fields are present in the test spec
def test_get_column_field_summary(tmp_path_factory):
    column_field_dir = tmp_path_factory.mktemp("column_field")
    converted_dir = tmp_path_factory.mktemp("converted")
    pipeline_dir = tmp_path_factory.mktemp("pipeline")

    resource = "resource"
    dataset = "address"
    endpoint_resource_info = {"resource": resource}

    specification_dir = "tests/data/specification"

    column_field_headers = ["column", "field"]
    column_field_rows = [
        {"column": "column1", "field": "address"},
        {"column": "column2", "field": "address-text"},
        {"column": "column3", "field": "end-date"},
        {"column": "column4", "field": "entry-date"},
        {"column": "column5", "field": "latitude"},
        {"column": "column6", "field": "longitude"},
        {"column": "column7", "field": "name"},
        {"column": "column8", "field": "notes"},
        {"column": "column9", "field": "reference"},
    ]
    with open(os.path.join(column_field_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=column_field_headers)
        writer.writeheader()
        writer.writerows(column_field_rows)

    converted_headers = [
        "column1",
        "column2",
        "column3",
        "column4",
        "column5",
        "column6",
        "column7",
        "column8",
        "column9",
        "new_column",
    ]

    with open(os.path.join(converted_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=converted_headers)
        writer.writeheader()

    transform_file = pipeline_dir / "transform.csv"
    transform_headers = ["dataset", "field", "replacement-field"]

    with open(os.path.join(transform_file), "w") as f:
        writer = csv.DictWriter(f, fieldnames=transform_headers)
        writer.writeheader()

    column_field_summary = get_column_field_summary(
        dataset,
        endpoint_resource_info,
        column_field_dir,
        converted_dir,
        specification_dir,
        pipeline_dir,
    )

    assert "Unmapped Columns:\nnew_column" in column_field_summary
    assert "Unmapped Fields:\nstart-date" in column_field_summary


def test_column_field_summary_no_reference(tmp_path_factory):
    column_field_dir = tmp_path_factory.mktemp("column_field")
    converted_dir = tmp_path_factory.mktemp("converted")
    pipeline_dir = tmp_path_factory.mktemp("pipeline")

    resource = "resource"
    dataset = "address"
    endpoint_resource_info = {"resource": resource}

    specification_dir = "tests/data/specification"

    column_field_headers = ["column", "field"]
    column_field_rows = [
        {"column": "column1", "field": "address"},
        {"column": "column2", "field": "address-text"},
        {"column": "column3", "field": "end-date"},
        {"column": "column4", "field": "entry-date"},
        {"column": "column5", "field": "latitude"},
        {"column": "column6", "field": "longitude"},
        {"column": "column7", "field": "name"},
        {"column": "column8", "field": "notes"},
    ]
    with open(os.path.join(column_field_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=column_field_headers)
        writer.writeheader()
        writer.writerows(column_field_rows)

    converted_headers = [
        "column1",
        "column2",
        "column3",
        "column4",
        "column5",
        "column6",
        "column7",
        "column8",
    ]

    with open(os.path.join(converted_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=converted_headers)
        writer.writeheader()

    transform_file = pipeline_dir / "transform.csv"
    transform_headers = ["dataset", "field", "replacement-field"]

    with open(os.path.join(transform_file), "w") as f:
        writer = csv.DictWriter(f, fieldnames=transform_headers)
        writer.writeheader()

    with pytest.raises(ValueError) as error:
        get_column_field_summary(
            dataset,
            endpoint_resource_info,
            column_field_dir,
            converted_dir,
            specification_dir,
            pipeline_dir,
        )

    assert "Reference not found in the mapped fields" in str(error)


def test_get_column_field_summary_get_reference_and_encoding(tmp_path_factory):
    column_field_dir = tmp_path_factory.mktemp("column_field")
    converted_dir = tmp_path_factory.mktemp("converted")
    collection_dir = tmp_path_factory.mktemp("collection")
    pipeline_dir = tmp_path_factory.mktemp("pipeline")

    resource = "resource"
    dataset = "brownfield-land"

    resource_dir = collection_dir / dataset / "resource"
    resource_dir.mkdir(parents=True, exist_ok=True)
    endpoint_resource_info = {
        "resource": resource,
        "resource_path": resource_dir / resource,
    }
    specification_dir = "tests/data/specification"

    column_field_headers = ["column", "field"]
    column_field_rows = [
        {"column": "SiteReference", "field": "SiteReference"},
        {"column": "SiteNameAddress", "field": "SiteNameAddress"},
    ]
    with open(os.path.join(column_field_dir, resource + ".csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=column_field_headers)
        writer.writeheader()
        writer.writerows(column_field_rows)

    with open(
        endpoint_resource_info["resource_path"], "w", encoding="windows-1252"
    ) as f:
        f.write("""SiteReference,SiteNameAddress\nref,"name – BFL""")

    transform_file = pipeline_dir / "transform.csv"
    transform_headers = ["dataset", "field", "replacement-field"]
    transform_rows = [
        {
            "dataset": "brownfield-land",
            "field": "SiteReference",
            "replacement-field": "reference",
        },
    ]

    with open(os.path.join(transform_file), "w") as f:
        writer = csv.DictWriter(f, fieldnames=transform_headers)
        writer.writeheader()
        writer.writerows(transform_rows)

    column_field_summary = get_column_field_summary(
        dataset,
        endpoint_resource_info,
        column_field_dir,
        converted_dir,
        specification_dir,
        pipeline_dir,
    )

    assert "Unmapped Columns:\nNo unmapped columns!" in column_field_summary
    assert "Unmapped Fields:\nNo unmapped fields!" in column_field_summary


def test_get_existing_endpoints_summary(tmp_path):
    collection_dir = os.path.join(tmp_path, "collection")
    os.makedirs(collection_dir, exist_ok=True)
    endpoints = [
        {
            "endpoint": "test",
            "endpoint-url": "test1.com",
            "parameters": "",
            "plugin": "",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        },
        {
            "endpoint": "test2",
            "endpoint-url": "test2.com",
            "parameters": "",
            "plugin": "",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        },
        {
            "endpoint": "test3",
            "endpoint-url": "test3.com",
            "parameters": "",
            "plugin": "",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        },
    ]

    with open(os.path.join(collection_dir, "endpoint.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=endpoints[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(endpoints)

    sources = [
        {
            "source": "test1",
            "endpoint": "test",
            "attribution": "",
            "collection": "test",
            "documentation-url": "testing.com",
            "licence": "test",
            "organisation": "test-org1",
            "pipelines": "test",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        },
        {
            "source": "test2",
            "endpoint": "test2",
            "attribution": "",
            "collection": "test",
            "documentation-url": "testing.com",
            "licence": "test",
            "organisation": "test-org1",
            "pipelines": "test",
            "entry-date": "2020-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        },
        {
            "source": "test3",
            "endpoint": "test3",
            "attribution": "",
            "collection": "test",
            "documentation-url": "testing.com",
            "licence": "test",
            "organisation": "test-org1",
            "pipelines": "test",
            "entry-date": "2020-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        },
    ]

    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=sources[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(sources)

    endpoint_resource_info = {
        "source": "test3",
        "endpoint": "test3",
        "organisation": "test-org1",
        "pipelines": "test",
    }

    collection = Collection(directory=collection_dir)
    collection.load()

    existing_endpoints_summary, existing_sources = get_existing_endpoints_summary(
        endpoint_resource_info, collection, "test"
    )

    assert "entry-date, endpoint-url" in existing_endpoints_summary
    assert "2019-01-01, test1.com" in existing_endpoints_summary
    assert "2020-01-01, test2.com" in existing_endpoints_summary

    assert "2020-01-01, test3.com" not in existing_endpoints_summary

    assert existing_sources[0]["source"] == "test1"
    assert existing_sources[1]["source"] == "test2"


def test_get_existing_endpoints_summary_no_dataset_match(tmp_path):
    collection_dir = os.path.join(tmp_path, "collection")
    os.makedirs(collection_dir, exist_ok=True)
    endpoints = [
        {
            "endpoint": "test",
            "endpoint-url": "test1.com",
            "parameters": "",
            "plugin": "",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        }
    ]

    with open(os.path.join(collection_dir, "endpoint.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=endpoints[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(endpoints)

    sources = [
        {
            "source": "test1",
            "endpoint": "test",
            "attribution": "",
            "collection": "test",
            "documentation-url": "testing.com",
            "licence": "test",
            "organisation": "test-org1",
            "pipelines": "test",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        }
    ]

    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=sources[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(sources)

    endpoint_resource_info = {
        "source": "test3",
        "organisation": "test-org1",
        "pipelines": "test",
    }

    collection = Collection(directory=collection_dir)
    collection.load()

    existing_endpoints_summary, existing_sources = get_existing_endpoints_summary(
        endpoint_resource_info, collection, "wrong_dataset"
    )

    assert not existing_endpoints_summary
    assert not existing_sources


def test_get_existing_endpoints_summary_no_organisation_match(tmp_path):
    collection_dir = os.path.join(tmp_path, "collection")
    os.makedirs(collection_dir, exist_ok=True)
    endpoints = [
        {
            "endpoint": "test",
            "endpoint-url": "test1.com",
            "parameters": "",
            "plugin": "",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        }
    ]

    with open(os.path.join(collection_dir, "endpoint.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=endpoints[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(endpoints)

    sources = [
        {
            "source": "test1",
            "endpoint": "test",
            "attribution": "",
            "collection": "test",
            "documentation-url": "testing.com",
            "licence": "test",
            "organisation": "wrong-organisation",
            "pipelines": "test",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        }
    ]

    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=sources[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(sources)

    endpoint_resource_info = {
        "source": "test3",
        "organisation": "test-org1",
        "pipelines": "test",
    }

    collection = Collection(directory=collection_dir)
    collection.load()

    existing_endpoints_summary, existing_sources = get_existing_endpoints_summary(
        endpoint_resource_info, collection, "test"
    )

    assert not existing_endpoints_summary
    assert not existing_sources


@pytest.mark.parametrize(
    "endpoints,sources",
    [
        (
            [
                {
                    "endpoint": "test",
                    "endpoint-url": "test1.com",
                    "parameters": "",
                    "plugin": "",
                    "entry-date": "2019-01-01",
                    "start-date": "2019-01-01",
                    "end-date": "",
                }
            ],
            [
                {
                    "source": "test1",
                    "endpoint": "test",
                    "attribution": "",
                    "collection": "test",
                    "documentation-url": "testing.com",
                    "licence": "test",
                    "organisation": "test-org1",
                    "pipelines": "test",
                    "entry-date": "2019-01-01",
                    "start-date": "2019-01-01",
                    "end-date": "ended!",
                }
            ],
        ),
        (
            [
                {
                    "endpoint": "test",
                    "endpoint-url": "test1.com",
                    "parameters": "",
                    "plugin": "",
                    "entry-date": "2019-01-01",
                    "start-date": "2019-01-01",
                    "end-date": "ended!",
                }
            ],
            [
                {
                    "source": "test1",
                    "endpoint": "test",
                    "attribution": "",
                    "collection": "test",
                    "documentation-url": "testing.com",
                    "licence": "test",
                    "organisation": "test-org1",
                    "pipelines": "test",
                    "entry-date": "2019-01-01",
                    "start-date": "2019-01-01",
                    "end-date": "",
                }
            ],
        ),
    ],
)
def test_get_existing_endpoints_summary_ended_endpoint_or_source(
    endpoints, sources, tmp_path
):
    collection_dir = os.path.join(tmp_path, "collection")
    os.makedirs(collection_dir, exist_ok=True)

    with open(os.path.join(collection_dir, "endpoint.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=endpoints[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(endpoints)

    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=sources[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(sources)

    endpoint_resource_info = {
        "source": "test3",
        "endpoint": "test3",
        "organisation": "test-org1",
        "pipelines": "test",
    }

    collection = Collection(directory=collection_dir)
    collection.load()

    existing_endpoints_summary, existing_sources = get_existing_endpoints_summary(
        endpoint_resource_info, collection, "test"
    )
    assert "test1.com" in existing_endpoints_summary

    assert existing_sources[0]["source"] == "test1"


def test_get_existing_endpoints_summary_ended_endpoint_and_source(tmp_path):
    collection_dir = os.path.join(tmp_path, "collection")
    os.makedirs(collection_dir, exist_ok=True)

    endpoints = [
        {
            "endpoint": "test",
            "endpoint-url": "test1.com",
            "parameters": "",
            "plugin": "",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "ended!",
        }
    ]
    with open(os.path.join(collection_dir, "endpoint.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=endpoints[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(endpoints)

    sources = [
        {
            "source": "test1",
            "endpoint": "test",
            "attribution": "",
            "collection": "test",
            "documentation-url": "testing.com",
            "licence": "test",
            "organisation": "test-org1",
            "pipelines": "test",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "ended!",
        }
    ]

    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=sources[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(sources)

    endpoint_resource_info = {
        "source": "test3",
        "endpoint": "test3",
        "organisation": "test-org1",
        "pipelines": "test",
    }

    collection = Collection(directory=collection_dir)
    collection.load()

    existing_endpoints_summary, existing_sources = get_existing_endpoints_summary(
        endpoint_resource_info, collection, "test"
    )
    assert not existing_endpoints_summary

    assert not existing_sources


def test_get_existing_endpoints_source_with_no_endpoint(tmp_path):
    collection_dir = os.path.join(tmp_path, "collection")
    os.makedirs(collection_dir, exist_ok=True)
    endpoints = [
        {
            "endpoint": "differentendpoint",
            "endpoint-url": "test1.com",
            "parameters": "",
            "plugin": "",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        },
    ]

    with open(os.path.join(collection_dir, "endpoint.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=endpoints[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(endpoints)

    sources = [
        {
            "source": "test1",
            "endpoint": "",
            "attribution": "",
            "collection": "test",
            "documentation-url": "testing.com",
            "licence": "test",
            "organisation": "test-org1",
            "pipelines": "test",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        },
    ]

    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=sources[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(sources)

    endpoint_resource_info = {
        "source": "test3",
        "endpoint": "test3",
        "organisation": "test-org1",
        "pipelines": "test",
    }

    collection = Collection(directory=collection_dir)
    collection.load()

    existing_endpoints_summary, existing_sources = get_existing_endpoints_summary(
        endpoint_resource_info, collection, "test"
    )

    assert "WARNING: No endpoint found for source test1" in existing_endpoints_summary
    assert len(existing_sources) == 0


def test_get_existing_endpoints_ended_source_with_no_endpoint(tmp_path):
    collection_dir = os.path.join(tmp_path, "collection")
    os.makedirs(collection_dir, exist_ok=True)
    endpoints = [
        {
            "endpoint": "differentendpoint",
            "endpoint-url": "test1.com",
            "parameters": "",
            "plugin": "",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "",
        },
    ]

    with open(os.path.join(collection_dir, "endpoint.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=endpoints[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(endpoints)

    sources = [
        {
            "source": "test1",
            "endpoint": "",
            "attribution": "",
            "collection": "test",
            "documentation-url": "testing.com",
            "licence": "test",
            "organisation": "test-org1",
            "pipelines": "test",
            "entry-date": "2019-01-01",
            "start-date": "2019-01-01",
            "end-date": "end-date",
        },
    ]

    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=sources[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(sources)

    endpoint_resource_info = {
        "source": "test3",
        "endpoint": "test3",
        "organisation": "test-org1",
        "pipelines": "test",
    }

    collection = Collection(directory=collection_dir)
    collection.load()

    existing_endpoints_summary, existing_sources = get_existing_endpoints_summary(
        endpoint_resource_info, collection, "test"
    )

    assert not existing_endpoints_summary
    assert len(existing_sources) == 0


def test_download_dataset(tmp_path_factory, mocker):
    dataset = "dataset-one"
    specification_dir = "tests/data/specification"
    specification = Specification(specification_dir)
    # create temp cache dir
    cache_dir = tmp_path_factory.mktemp("cache")

    # mock api download url
    sqlite_file_path = "tests/data/dataset/central-activities-zone.sqlite3"
    with open(sqlite_file_path, "rb") as f:
        data = f.read()
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.request.headers = {"test": "test"}
    mock_response.headers = {"test": "test"}
    mock_response.content = data
    mocker.patch("requests.get", return_value=mock_response)

    download_dataset(dataset, specification, cache_dir)

    path = os.path.join(cache_dir, "dataset", f"{dataset}.sqlite3")
    assert os.path.exists(path)


def test_download_dataset_use_cache_dataset(tmp_path_factory, mocker):
    dataset = "dataset-one"
    specification_dir = "tests/data/specification"
    specification = Specification(specification_dir)
    # create temp cache dir
    cache_dir = tmp_path_factory.mktemp("cache")

    path = os.path.join(cache_dir, "dataset", f"{dataset}.sqlite3")
    # put db file in cache dir
    sqlite_file_path = "tests/data/dataset/central-activities-zone.sqlite3"
    os.makedirs(os.path.dirname(path))
    shutil.copy(sqlite_file_path, path)

    # mock user response
    mocker.patch(
        "digital_land.utils.add_data_utils.get_user_response", return_value=True
    )
    mock_get = mocker.patch("requests.get")

    download_dataset(dataset, specification, cache_dir)

    # assert requests.get was NOT called
    mock_get.assert_not_called()


def test_get_transformed_entities():
    output_path = tempfile.NamedTemporaryFile().name
    dataset_path = "tests/data/dataset/central-activities-zone.sqlite3"

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
    transformed_rows = [
        {
            "end-date": "",
            "entity": 2200001,
            "entry-date": "",
            "entry-number": 1,
            "fact": "fact1",
            "field": "field1",
            "priority": "",
            "reference-entity": "",
            "resource": "resource",
            "start-date": "",
            "value": "value1",
        },
        {
            "end-date": "",
            "entity": 2200002,
            "entry-date": "",
            "entry-number": 2,
            "fact": "fact2",
            "field": "field1",
            "priority": "",
            "reference-entity": "",
            "resource": "resource",
            "start-date": "",
            "value": "value1",
        },
    ]
    with open(output_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=transformed_headers)
        writer.writeheader()
        writer.writerows(transformed_rows)

    entities = get_transformed_entities(dataset_path, output_path)

    assert len(entities) == 2
    assert entities.iloc[0]["entity"] == 2200001
    assert entities.iloc[0]["reference"] == "CAZ00000001"
    assert entities.iloc[1]["entity"] == 2200002
    assert entities.iloc[1]["reference"] == "CAZ00000002"


def test_get_updated_entities_summary_new_entity():
    original_entity_df = pd.DataFrame.from_records(
        [
            {
                "end-date": "",
                "entity": 2200001,
                "dataset": "",
                "json": "json",
                "name": "name1",
                "reference": "ref1",
            }
        ]
    )
    updated_entity_df = pd.DataFrame.from_records(
        [
            {
                "end-date": "",
                "entity": 2200001,
                "dataset": "",
                "json": "json",
                "name": "name1",
                "reference": "ref1",
            },
            {
                "end-date": "",
                "entity": 2200002,
                "dataset": "",
                "json": "json",
                "name": "name2",
                "reference": "ref2",
            },
        ]
    )

    updated_entities_summary, diffs_df = get_updated_entities_summary(
        original_entity_df, updated_entity_df
    )

    assert len(diffs_df) == 5
    assert "end-date" in diffs_df["field"].values
    assert "dataset" in diffs_df["field"].values
    assert "name" in diffs_df["field"].values
    assert "reference" in diffs_df["field"].values
    assert "json" in diffs_df["field"].values

    assert "original_value" in diffs_df.columns
    assert all(not value for value in diffs_df["original_value"].values)

    assert "updated_value" in diffs_df.columns
    assert diffs_df[diffs_df["field"] == "name"]["updated_value"].values[0] == "name2"

    assert all(value for value in diffs_df["new_entity"].values)

    assert (
        "Entity: 2200002, Fields changed: end-date, dataset, json, name, reference"
        in updated_entities_summary
    )
    assert "Entity: 2200001" not in updated_entities_summary


def test_get_updated_entities_summary_updated_entity():
    original_entity_df = pd.DataFrame.from_records(
        [
            {
                "end-date": "",
                "entity": 2200001,
                "dataset": "",
                "json": "json",
                "name": "name1",
                "reference": "ref1",
            }
        ]
    )
    updated_entity_df = pd.DataFrame.from_records(
        [
            {
                "end-date": "updated end date",
                "entity": 2200001,
                "dataset": "",
                "json": "json",
                "name": "updated name",
                "reference": "ref1",
            }
        ]
    )

    updated_entities_summary, diffs_df = get_updated_entities_summary(
        original_entity_df, updated_entity_df
    )

    assert len(diffs_df) == 2

    assert diffs_df[diffs_df["field"] == "name"]["original_value"].values[0] == "name1"
    assert (
        diffs_df[diffs_df["field"] == "name"]["updated_value"].values[0]
        == "updated name"
    )

    assert diffs_df[diffs_df["field"] == "end-date"]["original_value"].values[0] == ""
    assert (
        diffs_df[diffs_df["field"] == "end-date"]["updated_value"].values[0]
        == "updated end date"
    )

    assert not all(value == "" for value in diffs_df["new_entity"].values)

    assert "Entity: 2200001, Fields changed: end-date, name" in updated_entities_summary


def test_get_updated_entities_summary_no_updates():
    original_entity_df = pd.DataFrame.from_records(
        [
            {
                "end-date": "",
                "entity": 2200001,
                "dataset": "",
                "json": "json",
                "name": "name1",
                "reference": "ref1",
            }
        ]
    )

    updated_entity_df = pd.DataFrame.from_records(
        [
            {
                "end-date": "",
                "entity": 2200001,
                "dataset": "",
                "json": "json",
                "name": "name1",
                "reference": "ref1",
            }
        ]
    )

    updated_entities_summary, diffs_df = get_updated_entities_summary(
        original_entity_df, updated_entity_df
    )

    assert "No differences found in updated dataset" in updated_entities_summary
    assert not diffs_df


def test_get_updated_entities_summary_updated_entity_none_agnostic():
    original_entity_df = pd.DataFrame.from_records(
        [
            {
                "end-date": "",
                "entity": 2200001,
                "dataset": "",
                "json": "json",
                "name": None,
                "reference": "ref1",
            }
        ]
    )
    updated_entity_df = pd.DataFrame.from_records(
        [
            {
                "end-date": None,
                "entity": 2200001,
                "dataset": "",
                "json": "json",
                "name": "",
                "reference": "ref1",
            }
        ]
    )

    updated_entities_summary, diffs_df = get_updated_entities_summary(
        original_entity_df, updated_entity_df
    )

    assert "No differences found in updated dataset" in updated_entities_summary
    assert not diffs_df
