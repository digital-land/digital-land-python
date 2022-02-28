import pytest

from digital_land.register import hash_value
from digital_land.collection import Collection

test_collection_dir = "tests/data/collection"


@pytest.mark.skip("Currently failing")
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
