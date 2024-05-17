import pandas as pd

from digital_land.register import hash_value, Item
from digital_land.collection import Collection, LogStore
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


def test_retire_endpoints_and_sources(tmp_path):

    # Create a temporary directory for the test collection
    test_collection_dir = tmp_path / "collection"
    test_collection_dir.mkdir()

    # Create mock endpoint and source CSV files
    endpoint_csv_path = test_collection_dir / "endpoint.csv"
    source_csv_path = test_collection_dir / "source.csv"

    # Mock dataframes for endpoint and source CSV files
    endpoint_data = {
        "endpoint": ["endpoint1", "endpoint2", "endpoint3"],
        "end-date": ["", "", ""],
    }
    source_data = {
        "source": ["source1", "source2", "source3"],
        "end-date": ["", "", ""],
    }

    # Write mock dataframes to CSV files
    pd.DataFrame(endpoint_data).to_csv(endpoint_csv_path, index=False)
    pd.DataFrame(source_data).to_csv(source_csv_path, index=False)

    # Instantiate Collection object
    collection = Collection(directory=str(test_collection_dir))

    # Mock dataframe for collection_df_to_retire
    collection_df_to_retire = pd.DataFrame(
        {"endpoint": ["endpoint1", "endpoint3"], "source": ["source2", "source3"]}
    )

    # Call the retire_endpoints_and_sources method
    collection.retire_endpoints_and_sources(collection, collection_df_to_retire)

    # Read updated endpoint and source CSV files
    updated_endpoint_df = pd.read_csv(endpoint_csv_path)
    updated_source_df = pd.read_csv(source_csv_path)

    # Get today's date
    today_date = datetime.now().strftime("%Y-%m-%d")

    # Check if the end-date column is updated correctly
    expected_endpoint_data = {
        "endpoint": ["endpoint1", "endpoint2", "endpoint3"],
        "end-date": [today_date, "", today_date],
    }
    expected_source_data = {
        "source": ["source1", "source2", "source3"],
        "end-date": ["", today_date, today_date],
    }

    assert updated_endpoint_df.to_dict() == expected_endpoint_data
    assert updated_source_df.to_dict() == expected_source_data
