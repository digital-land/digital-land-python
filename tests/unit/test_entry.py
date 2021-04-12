import datetime

import pytest

from digital_land.model.entry import Entry
from digital_land.model.fact import Fact


def test_init():
    resource = "abc123"
    line_num = 1
    entry_date = "2021-03-19"
    data = {"a": "b", "slug": "/abc", "entry-date": entry_date}

    entry = Entry(data, resource, line_num)

    assert entry.data == data
    assert entry.resource == resource
    assert entry.line_num == line_num
    assert entry.entry_date == datetime.date(2021, 3, 19)
    assert entry.slug == "/abc"


def test_missing_slug_exception():
    data = {"a": "b"}
    resource = "abc123"
    line_num = 1

    with pytest.raises(ValueError, match="^Entry missing slug field$"):
        Entry(data, resource, line_num)


def test_entry_in_future_exception():
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    data = {"a": "b", "slug": "/abc", "entry-date": tomorrow.isoformat()}
    resource = "abc123"
    line_num = 1

    with pytest.raises(ValueError, match="^entry-date cannot be in the future$"):
        Entry(data, resource, line_num)


def test_to_facts():
    slug = "/abc"
    data = {"a": "b", "c": "d", "e": "f", "slug": slug, "entry-date": "2012-03-19"}
    entry = Entry(data, "abc123", 1)

    facts = entry.facts

    assert facts == set(
        [Fact(slug, "a", "b"), Fact(slug, "c", "d"), Fact(slug, "e", "f")]
    )


def test_from_facts():
    slug = "/abc"
    resource = "abc123"
    line_num = 1
    facts = set([Fact(slug, "a", "b"), Fact(slug, "c", "d"), Fact(slug, "e", "f")])

    entry = Entry.from_facts(slug, facts, resource, line_num, "2021-03-19")

    assert entry.slug == slug
    assert entry.entry_date == datetime.date(2021, 3, 19)
    assert entry.resource == resource
    assert entry.line_num == line_num
    assert entry.data == {
        "a": "b",
        "c": "d",
        "e": "f",
    }


def test_equality():
    entry_1 = Entry(
        {"a": "b", "c": "d", "e": "f", "slug": "/abc", "entry-date": "2012-03-19"},
        "abc123",
        1,
    )
    entry_2 = Entry(
        {"c": "d", "e": "f", "a": "b", "slug": "/abc", "entry-date": "2012-03-19"},
        "abc123",
        1,
    )
    entry_3 = Entry(
        {"e": "f", "a": "b", "slug": "/abc", "entry-date": "2012-03-19"},
        "abc123",
        1,
    )

    assert entry_1 == entry_2  # ordering of the dict items is ignored
    assert entry_1 != entry_3


def test_ordering():
    entry_1 = Entry(
        {"a": "b", "slug": "/abc", "entry-date": "2012-01-01"},
        "abc123",
        1,
    )
    entry_2 = Entry(
        {"c": "d", "slug": "/abc", "entry-date": "2016-06-01"},
        "def456",
        20,
    )
    entry_3 = Entry(
        {"e": "f", "slug": "/abc", "entry-date": "2021-03-25"},
        "xyz",
        999,
    )

    assert sorted([entry_2, entry_3, entry_1]) == [entry_1, entry_2, entry_3]
