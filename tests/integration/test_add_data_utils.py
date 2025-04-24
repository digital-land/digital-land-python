import csv
from datetime import datetime
import os
import pytest

from digital_land.collection import Collection
from digital_land.utils.add_data_utils import (
    clear_log,
    get_column_field_summary,
    get_entity_summary,
    get_existing_endpoints_summary,
    get_issue_summary,
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
