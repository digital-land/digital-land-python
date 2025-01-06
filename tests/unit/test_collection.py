import pytest
from digital_land.register import hash_value, Item
from digital_land.collection import Collection, LogStore, ResourceLogStore, CSVStore
from digital_land.schema import Schema
from datetime import datetime

test_collection_dir = "tests/data/collection"


def test_collection():
    collection = Collection()
    collection.load(directory=test_collection_dir)

    url = "https://example.com/register/1.csv"
    endpoint = hash_value(url)

    assert (
        endpoint == "50335d6703d9bebb683f1b27e02ad17e991ff527bed7e0ab620cd1b6e4b5689e"
    )
    assert endpoint in collection.endpoint.records
    assert endpoint in collection.source.records

    record = collection.endpoint.records[endpoint]

    assert len(record) == 4

    for entry in record:
        assert entry["endpoint"] == endpoint
        assert entry["endpoint-url"] == url

    assert record[0]["start-date"] == ""
    assert record[1]["start-date"] == "2020-01-15"
    assert record[2]["start-date"] == "2020-01-12"
    assert record[3]["start-date"] == "2020-01-12"
    assert record[3]["end-date"] == "2020-02-01"
    collection.load_log_items(directory=test_collection_dir)

    resources = collection.resource.records
    assert len(resources) == 2

    assert collection.resource_endpoints(
        "ae191fd3dc6a892d82337d9045bf4c1043804a1961131b0a9271280f86b6a8cf"
    ) == ["7f72ae4d3152d220bb1786923c134cf286d4c42720c98dd269d067d461e18b70"]

    assert collection.resource_organisations(
        "ae191fd3dc6a892d82337d9045bf4c1043804a1961131b0a9271280f86b6a8cf"
    ) == ["organisation:2"]


def test_check_item_path():
    """
    check_item_path of LogStore was changed from being Linux/Unix specific,
    to being cross-platform friendly - by use of Path
    The method was also changed to return a boolean, to make it easier to Test
    """

    test_file_dir = "tests/data/collection/log/2020-02-12/"
    test_file_name = (
        "50335d6703d9bebb683f1b27e02ad17e991ff527bed7e0ab620cd1b6e4b5689e.json"
    )

    expected_endpoint = (
        "50335d6703d9bebb683f1b27e02ad17e991ff527bed7e0ab620cd1b6e4b5689e"
    )
    unexpected_endpoint = "ABCDE"

    test_file_path = test_file_dir + test_file_name
    test_file = open(test_file_path).read()
    item = Item()
    item.unpack(test_file)

    log_store = LogStore(Schema("log"))

    # Good data test
    item["endpoint"] = expected_endpoint
    assert log_store.check_item_path(item, test_file_path) is True

    # Bad data test
    item["endpoint"] = unexpected_endpoint
    assert log_store.check_item_path(item, test_file_path) is False


def test_format_date():
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d")

    for check, dates in {
        "2023-08-01": ["2023-8-1", "2023-08-01", "1/8/2023", "01/08/2023", 20230801],
        now_str: [now, "NotADate", None, list()],
    }.items():
        for date in dates:
            print(date)
            assert Collection.format_date(date) == check


def test_endpoint_source_mismatch():
    log = LogStore(Schema("log"))
    log.add_entry(
        {
            "endpoint": "abc123",
            "entry-date": "2025-01-06",
            "resource": "aaa",
            "bytes": "1024",
        }
    )

    # This one matches
    source = CSVStore(Schema("source"))
    source.add_entry({"endpoint": "abc123", "organisation": "test-org"})

    resource = ResourceLogStore(Schema("resource"))
    resource.load(log, source)

    # This one doesn't
    source = CSVStore(Schema("source"))
    source.add_entry({"endpoint": "abc124", "organisation": "test-org"})

    resource = ResourceLogStore(Schema("resource"))
    with pytest.raises(RuntimeError):
        resource.load(log, source)
