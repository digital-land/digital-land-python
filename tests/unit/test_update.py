import pytest

from collections import defaultdict

from digital_land.schema import Schema
from digital_land.store.memory import MemoryStore

from digital_land.update import (
    add_endpoint,
    add_source,
)


@pytest.fixture()
def log_entries():
    return [
        {
            "endpoint": "AAA",
            "endpoint-url": "www.aaa.com",
            "entry-date": "2018-06-25T13:41:49.222813",
            "resource": "",
            "status": "404",
        },
        {
            "endpoint": "BBB",
            "endpoint-url": "www.bbb.com",
            "entry-date": "2019-05-10T13:41:49.222813",
            "resource": "12345",
            "status": "200",
        },
        {
            "endpoint": "CCC",
            "endpoint-url": "www.ccc.com",
            "entry-date": "2020-04-15T13:41:49.222813",
            "exception": "something failed",
        },
        {
            "endpoint": "CCC",
            "endpoint-url": "www.ccc.com",
            "entry-date": "2020-05-20T13:41:49.222813",
            "exception": "something failed",
        },
        {
            "endpoint": "DDD",
            "endpoint-url": "www.ddd.com",
            "entry-date": "2020-10-20T13:41:49.222813",
            "status": "200",
        },
        {
            "endpoint": "EEE",
            "endpoint-url": "www.eee.com",
            "entry-date": "2020-10-25T13:41:49.222813",
            "status": "500",
        },
    ]


@pytest.fixture()
def endpoint_entries():
    return [
        {"endpoint": "AAA", "end-date": ""},
        {"endpoint": "BBB", "end-date": ""},
        {"endpoint": "CCC", "end-date": ""},
        {"endpoint": "DDD", "end-date": "2020-10-21T13:41:49.222813"},
        {"endpoint": "EEE", "end-date": ""},
    ]


@pytest.fixture()
def source_entries():
    return [
        {"organisation": "AZK", "endpoint": "AAA", "end-date": ""},
        {
            "organisation": "BYT",
            "endpoint": "BBB",
            "end-date": "2020-10-21T13:41:49.222813",
        },
        {"organisation": "CXJ", "endpoint": "CCC", "end-date": ""},
        {
            "organisation": "CXJ",
            "endpoint": "DDD",
            "end-date": "2020-10-21T13:41:49.222813",
        },
        {"organisation": "DWF", "endpoint": "BBB", "end-date": ""},
    ]


@pytest.fixture()
def source_register(source_entries):
    register = MemoryStore(Schema("source"))
    for entry in source_entries:
        register.add_entry(entry)
    return register


@pytest.fixture()
def endpoint_register(endpoint_entries):
    register = MemoryStore(Schema("endpoint"))
    for entry in endpoint_entries:
        register.add_entry(entry)
    return register


def test_add_endpoint_order(endpoint_register):
    expected_result = endpoint_register.entries.copy()
    test_key = "BBC"
    test_url = "www.someurl.com"
    entry = defaultdict(str)
    entry.update(
        {
            "endpoint-url": test_url,
            "endpoint": test_key,
            "organisation": "DWF",
            "documentation-url": "www.doc.com",
        }
    )
    add_endpoint(entry, endpoint_register)
    assert expected_result == endpoint_register.entries[0:-1]
    assert endpoint_register.entries[-1]["endpoint"] == test_key
    assert endpoint_register.entries[-1]["endpoint-url"] == test_url


def test_add_endpoint_existing_end_date(endpoint_register):
    test_key = "DDD"
    expected_result = endpoint_register.entries.copy()
    entry = defaultdict(str)
    entry.update(
        {
            "endpoint-url": "www.someurl.com",
            "endpoint": test_key,
            "organisation": "DWF",
            "documentation-url": "www.doc.com",
        }
    )
    add_endpoint(entry, endpoint_register)
    assert expected_result == endpoint_register.entries[0:-1]
    assert endpoint_register.entries[-1]["endpoint"] == test_key


def test_add_source(source_register):
    expected_result = source_register.entries.copy()
    entry = defaultdict(str)
    entry.update(
        {
            "endpoint-url": "www.test.com",
            "endpoint": "EEE",
            "organisation": "CXJ",
            "documentation-url": "www.doc.com",
            "start-date": "",
        }
    )
    add_source(entry, source_register)
    assert expected_result == source_register.entries[0:-1]
    assert source_register.entries[-1]["endpoint"] == entry["endpoint"]
    assert (
        source_register.entries[-1]["documentation-url"] == entry["documentation-url"]
    )
    assert source_register.entries[-1]["start-date"] == entry["start-date"]


def test_add_source_existing_endpoint(source_register):
    expected_result = source_register.entries.copy()
    entry = defaultdict(str)
    entry.update(
        {
            "endpoint-url": "www.test.com",
            "endpoint": "BBB",
            "organisation": "ERT",
            "documentation-url": "www.doc.com",
        }
    )
    add_source(entry, source_register)
    assert expected_result == source_register.entries[0:-1]
    assert source_register.entries[-1]["endpoint"] == entry["endpoint"]
    assert (
        source_register.entries[-1]["documentation-url"] == entry["documentation-url"]
    )


def test_add_source_existing_end_date(source_register):
    expected_result = source_register.entries.copy()
    entry = defaultdict(str)
    entry.update(
        {
            "endpoint-url": "www.test.com",
            "endpoint": "BBB",
            "organisation": "BYT",
            "documentation-url": "www.doc.com",
        }
    )
    add_source(entry, source_register)
    assert expected_result == source_register.entries[0:-1]
    assert source_register.entries[-1]["endpoint"] == entry["endpoint"]
