import pytest

from datetime import date
from digital_land.register import Item
from digital_land.collection import SourceRegister, EndpointRegister, LogItem
from digital_land.update import (
    has_collected_resource,
    get_failing_endpoints,
    get_entries_between_keys,
    add_new_endpoint,
    add_new_source,
)


@pytest.fixture()
def log_entries():
    return [
        LogItem(
            {
                "endpoint": "AAA",
                "url": "www.aaa.com",
                "entry-date": "2018-06-25T13:41:49.222813",
                "resource": "",
                "status": "404",
            }
        ),
        LogItem(
            {
                "endpoint": "BBB",
                "url": "www.bbb.com",
                "entry-date": "2019-05-10T13:41:49.222813",
                "resource": "12345",
                "status": "200",
            }
        ),
        LogItem(
            {
                "endpoint": "CCC",
                "url": "www.ccc.com",
                "entry-date": "2020-04-15T13:41:49.222813",
                "exception": "something failed",
            }
        ),
        LogItem(
            {
                "endpoint": "CCC",
                "url": "www.ccc.com",
                "entry-date": "2020-05-20T13:41:49.222813",
                "exception": "something failed",
            }
        ),
        LogItem(
            {
                "endpoint": "DDD",
                "url": "www.ddd.com",
                "entry-date": "2020-10-20T13:41:49.222813",
                "status": "200",
            }
        ),
        LogItem(
            {
                "endpoint": "EEE",
                "url": "www.eee.com",
                "entry-date": "2020-10-25T13:41:49.222813",
                "status": "500",
            }
        ),
    ]


@pytest.fixture()
def endpoint_entries():
    return [
        Item({"endpoint": "AAA", "end-date": ""}),
        Item({"endpoint": "BBB", "end-date": ""}),
        Item({"endpoint": "CCC", "end-date": ""}),
        Item({"endpoint": "DDD", "end-date": "2020-10-21T13:41:49.222813"}),
        Item({"endpoint": "EEE", "end-date": ""}),
    ]


@pytest.fixture()
def source_entries():
    return [
        Item({"organisation": "AZK", "endpoint": "AAA", "end-date": ""}),
        Item(
            {
                "organisation": "BYT",
                "endpoint": "BBB",
                "end-date": "2020-10-21T13:41:49.222813",
            }
        ),
        Item({"organisation": "CXJ", "endpoint": "CCC", "end-date": ""}),
        Item(
            {
                "organisation": "CXJ",
                "endpoint": "DDD",
                "end-date": "2020-10-21T13:41:49.222813",
            }
        ),
        Item({"organisation": "DWF", "endpoint": "BBB", "end-date": ""}),
    ]


@pytest.fixture()
def source_register(source_entries):
    register = SourceRegister()
    for entry in source_entries:
        register.add(entry)
    return register


@pytest.fixture()
def endpoint_register(endpoint_entries):
    register = EndpointRegister()
    for entry in endpoint_entries:
        register.add(entry)
    return register


def test_get_entries_between_keys():
    register = [1, 1, 4, 5, 6, 7, 9, 10, 10]
    start_idx, end_idx = get_entries_between_keys(
        1, 10, len(register), lambda idx: register[idx]
    )
    assert (start_idx, end_idx) == (0, 8)

    start_idx, end_idx = get_entries_between_keys(
        0, 8, len(register), lambda idx: register[idx]
    )
    assert (start_idx, end_idx) == (0, 5)

    start_idx, end_idx = get_entries_between_keys(
        3, 7, len(register), lambda idx: register[idx]
    )
    assert (start_idx, end_idx) == (2, 5)


def test_has_collected_resource_404(log_entries):
    # Return false due to 404
    result, reason = has_collected_resource(log_entries[0].item)
    assert result is False


def test_has_collected_resource_exception(log_entries):
    # Return false due to exception
    result, reason = has_collected_resource(log_entries[3].item)
    assert result is False


def test_has_collected_resource_no_resource(log_entries):
    # Return false due to missing resource
    result, reason = has_collected_resource(log_entries[4].item)
    assert result is False


def test_has_collected_resource(log_entries):
    # Return true
    result, reason = has_collected_resource(log_entries[1].item)
    assert result is True


def test_get_failing_endpoints_filter_inactive(log_entries, endpoint_entries):
    # Test active endpoints
    failing_endpoints = get_failing_endpoints(
        log_entries, endpoint_entries, first_date=date(2017, 1, 1)
    )
    expected_result = {"AAA", "CCC", "EEE"}
    assert (
        set(failing_endpoints.keys()) == expected_result
        and len(failing_endpoints["CCC"]["failure_dates"]) == 2
    )


def test_get_failing_endpoints_filter_dates(log_entries, endpoint_entries):
    # Test date filtering
    failing_endpoints = get_failing_endpoints(
        log_entries, endpoint_entries, first_date=date(2020, 10, 23)
    )
    expected_result = {"EEE"}
    assert set(failing_endpoints.keys()) == expected_result


def test_add_new_endpoint_order(endpoint_register):
    expected_result = endpoint_register.entries.copy()
    test_key = "BBC"
    test_url = "www.someurl.com"
    entry = {
        "endpoint-url": test_url,
        "endpoint": test_key,
        "organisation": "DWF",
        "documentation-url": "www.doc.com",
    }
    add_new_endpoint(entry, endpoint_register)
    assert expected_result == endpoint_register.entries[0:-1]
    assert endpoint_register.entries[-1].item["endpoint"] == test_key
    assert endpoint_register.entries[-1].item["endpoint-url"] == test_url


def test_add_new_endpoint_existing_no_op(endpoint_register):
    expected_result = endpoint_register.entries.copy()
    entry = {
        "endpoint-url": "www.eee.com",
        "endpoint": "EEE",
        "organisation": "DWF",
        "documentation-url": "www.doc.com",
    }
    add_new_endpoint(entry, endpoint_register)
    assert endpoint_register.entries == expected_result


def test_add_new_endpoint_existing_end_date(endpoint_register):
    test_key = "DDD"
    expected_result = endpoint_register.entries.copy()
    entry = {
        "endpoint-url": "www.someurl.com",
        "endpoint": test_key,
        "organisation": "DWF",
        "documentation-url": "www.doc.com",
    }
    add_new_endpoint(entry, endpoint_register)
    assert expected_result == endpoint_register.entries[0:-1]
    assert endpoint_register.entries[-1].item["endpoint"] == test_key


def test_add_new_source(source_register):
    expected_result = source_register.entries.copy()
    entry = {
        "endpoint-url": "www.test.com",
        "endpoint": "EEE",
        "organisation": "CXJ",
        "documentation-url": "www.doc.com",
        "start-date": "",
    }
    add_new_source(entry, source_register)
    assert expected_result == source_register.entries[0:-1]
    assert source_register.entries[-1].item["endpoint"] == entry["endpoint"]
    assert (
        source_register.entries[-1].item["documentation-url"]
        == entry["documentation-url"]
    )
    assert source_register.entries[-1].item["start-date"] == entry["start-date"]


def test_add_new_source_existing_endpoint(source_register):
    expected_result = source_register.entries.copy()
    entry = {
        "endpoint-url": "www.test.com",
        "endpoint": "BBB",
        "organisation": "ERT",
        "documentation-url": "www.doc.com",
    }
    add_new_source(entry, source_register)
    assert expected_result == source_register.entries[0:-1]
    assert source_register.entries[-1].item["endpoint"] == entry["endpoint"]
    assert (
        source_register.entries[-1].item["documentation-url"]
        == entry["documentation-url"]
    )


def test_add_new_source_existing_no_op(source_register):
    expected_result = source_register.entries.copy()
    entry = {
        "endpoint-url": "www.test.com",
        "endpoint": "BBB",
        "organisation": "DWF",
        "documentation-url": "www.doc.com",
    }
    add_new_source(entry, source_register)
    assert source_register.entries == expected_result


def test_add_new_source_existing_end_date(source_register):
    expected_result = source_register.entries.copy()
    entry = {
        "endpoint-url": "www.test.com",
        "endpoint": "BBB",
        "organisation": "BYT",
        "documentation-url": "www.doc.com",
    }
    add_new_source(entry, source_register)
    assert expected_result == source_register.entries[0:-1]
    assert source_register.entries[-1].item["endpoint"] == entry["endpoint"]
