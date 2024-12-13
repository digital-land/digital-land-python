import csv
from datetime import datetime
import os
import pytest

from digital_land.utils.add_data_utils import (
    clear_log,
    get_column_field_summary,
    get_entity_summary,
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

    resource = "resource"
    endpoint_resource_info = {"resource": resource}

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

    entity_summary = get_entity_summary(endpoint_resource_info, output_path, issue_dir)
    print(entity_summary)
    assert "Number of existing entities in resource: 4" in entity_summary
    assert "Number of new entities in resource: 2" in entity_summary
    assert "reference  line-number" in entity_summary
    assert "reference            1" in entity_summary
    assert "NaN            2" in entity_summary


# This test also tests the 'exclude fields' functionality
# as some of those fields are present in the test spec
def test_get_column_field_summary(tmp_path_factory):
    column_field_dir = tmp_path_factory.mktemp("column_field")
    converted_dir = tmp_path_factory.mktemp("converted")

    resource = "resource"
    pipeline = "address"
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

    column_field_summary = get_column_field_summary(
        pipeline,
        endpoint_resource_info,
        column_field_dir,
        converted_dir,
        specification_dir,
    )

    assert "Unmapped Columns:\nnew_column" in column_field_summary
    assert "Unmapped Fields:\nstart-date" in column_field_summary


def test_column_field_summary_no_reference(tmp_path_factory):
    column_field_dir = tmp_path_factory.mktemp("column_field")
    converted_dir = tmp_path_factory.mktemp("converted")

    resource = "resource"
    pipeline = "address"
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

    with pytest.raises(ValueError) as error:
        get_column_field_summary(
            pipeline,
            endpoint_resource_info,
            column_field_dir,
            converted_dir,
            specification_dir,
        )

    assert "Reference not found in the mapped fields" in str(error)
