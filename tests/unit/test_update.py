from datetime import date
from digital_land.register import Item
from digital_land.collection import LogItem
from digital_land.update import (
    has_collected_resource,
    get_failing_endpoints,
    get_entries_between_keys,
)

LOG_ENTRIES = [
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

ENDPOINT_ENTRIES = [
    Item({"endpoint": "DDD", "end-date": "2020-10-21T13:41:49.222813"}),
    Item({"endpoint": "AAA", "end-date": ""}),
    Item({"endpoint": "BBB", "end-date": ""}),
    Item({"endpoint": "CCC", "end-date": ""}),
    Item({"endpoint": "EEE", "end-date": ""}),
]


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


def test_has_collected_resource_404():
    # Return false due to 404
    result, reason = has_collected_resource(LOG_ENTRIES[0].item)
    assert result == False


def test_has_collected_resource_exception():
    # Return false due to exception
    result, reason = has_collected_resource(LOG_ENTRIES[3].item)
    assert result == False


def test_has_collected_resource_no_resource():
    # Return false due to missing resource
    result, reason = has_collected_resource(LOG_ENTRIES[4].item)
    assert result == False


def test_has_collected_resource():
    # Return true
    result, reason = has_collected_resource(LOG_ENTRIES[1].item)
    assert result == True


def test_get_failing_endpoints_filter_inactive():
    # Test active endpoints
    failing_endpoints = get_failing_endpoints(
        LOG_ENTRIES, ENDPOINT_ENTRIES, first_date=date(2017, 1, 1)
    )
    expected_result = {"AAA", "CCC", "EEE"}
    assert (
        set(failing_endpoints.keys()) == expected_result
        and len(failing_endpoints["CCC"]["failure_dates"]) == 2
    )


def test_get_failing_endpoints_filter_dates():
    # Test date filtering
    failing_endpoints = get_failing_endpoints(
        LOG_ENTRIES, ENDPOINT_ENTRIES, first_date=date(2020, 10, 23)
    )
    expected_result = {"EEE"}
    assert set(failing_endpoints.keys()) == expected_result
