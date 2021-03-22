from datetime import date

from digital_land.model.entity import Entity
from digital_land.model.entry import Entry


def test_snapshot_single_entry():
    entries = [
        Entry({"a": "b", "slug": "/one", "entry-date": "2020-01-01"}, "abc123", 1)
    ]
    entity = Entity(entries)

    assert entity.snapshot() == {"a": "b", "slug": "/one", "entry-date": "2020-01-01"}


def test_snapshot_multiple_entries_no_overlap():
    entries = [
        Entry({"a": "b", "slug": "/one", "entry-date": "2020-01-01"}, "abc123", 1),
        Entry({"c": "d", "slug": "/one", "entry-date": "2020-01-01"}, "def456", 10),
    ]
    entity = Entity(entries)

    assert entity.snapshot() == {
        "a": "b",
        "c": "d",
        "slug": "/one",
        "entry-date": "2020-01-01",
    }


def test_snapshot_multiple_entries_with_overlap():
    entries = [
        Entry({"a": "b", "slug": "/one", "entry-date": "2019-01-01"}, "abc123", 1),
        Entry({"c": "d", "slug": "/one", "entry-date": "2020-01-01"}, "def456", 10),
        Entry({"a": "e", "slug": "/one", "entry-date": "2021-01-01"}, "xzy789", 99),
    ]
    entity = Entity(entries)

    assert entity.snapshot() == {
        "a": "e",
        "c": "d",
        "slug": "/one",
        "entry-date": "2021-01-01",
    }


def test_snapshot_date():
    entries = [
        Entry({"a": "b", "slug": "/one", "entry-date": "2019-01-01"}, "abc123", 1),
        Entry({"c": "d", "slug": "/one", "entry-date": "2020-01-01"}, "def456", 10),
        Entry({"a": "e", "slug": "/one", "entry-date": "2021-01-01"}, "xzy789", 99),
    ]
    entity = Entity(entries)
    snapshot_date = date.fromisoformat("2020-06-01")

    assert entity.snapshot(snapshot_date) == {
        "a": "b",
        "c": "d",
        "slug": "/one",
        "entry-date": "2020-01-01",
    }


def test_change_history():
    entry_1 = Entry({"a": "b", "slug": "/one", "entry-date": "2019-01-01"}, "abc123", 1)
    entry_2 = Entry(
        {"a": "b", "slug": "/one", "entry-date": "2020-01-01"}, "def456", 10
    )
    entry_3 = Entry(
        {"a": "e", "slug": "/one", "entry-date": "2021-01-01"}, "xzy789", 99
    )
    entries = [entry_1, entry_2, entry_3]
    entity = Entity(entries)

    history = entity.change_history()

    assert len(history) == 2
    assert history == [entry_3, entry_1]
