from datetime import date

from digital_land.model.entity import Entity
from digital_land.model.entry import Entry


def test_schema():
    entity = Entity([], "conservation-area")
    assert entity.schema == "conservation-area"


def test_snapshot_single_entry():
    entries = [
        Entry(
            {"a": "b", "entity": 1, "slug": "/one", "entry-date": "2020-01-01"},
            "abc123",
            1,
        )
    ]
    entity = Entity(entries, "conservation-area")

    assert entity.snapshot() == {
        "a": "b",
        "entity": 1,
        "slug": "/one",
        "entry-date": "2020-01-01",
    }


def test_snapshot_multiple_entries_no_overlap():
    entries = [
        Entry(
            {"a": "b", "entity": 1, "slug": "/one", "entry-date": "2020-01-01"},
            "abc123",
            1,
        ),
        Entry(
            {"c": "d", "entity": 1, "slug": "/one", "entry-date": "2020-01-01"},
            "def456",
            10,
        ),
    ]
    entity = Entity(entries, "conservation-area")

    assert entity.snapshot() == {
        "a": "b",
        "c": "d",
        "entity": 1,
        "slug": "/one",
        "entry-date": "2020-01-01",
    }


def test_snapshot_multiple_entries_with_overlap():
    entries = [
        Entry(
            {"a": "b", "entity": 1, "slug": "/one", "entry-date": "2019-01-01"},
            "abc123",
            1,
        ),
        Entry(
            {"c": "d", "entity": 1, "slug": "/one", "entry-date": "2020-01-01"},
            "def456",
            10,
        ),
        Entry(
            {"a": "e", "entity": 1, "slug": "/one", "entry-date": "2021-01-01"},
            "xzy789",
            99,
        ),
    ]
    entity = Entity(entries, "conservation-area")

    assert entity.snapshot() == {
        "a": "e",
        "c": "d",
        "entity": 1,
        "slug": "/one",
        "entry-date": "2021-01-01",
    }


def test_snapshot_date():
    entries = [
        Entry(
            {"a": "b", "entity": 1, "slug": "/one", "entry-date": "2019-01-01"},
            "abc123",
            1,
        ),
        Entry(
            {"c": "d", "entity": 2, "slug": "/one", "entry-date": "2020-01-01"},
            "def456",
            10,
        ),
        Entry(
            {"a": "e", "entity": 2, "slug": "/one", "entry-date": "2021-01-01"},
            "xzy789",
            99,
        ),
    ]
    entity = Entity(entries, "conservation-area")
    snapshot_date = date.fromisoformat("2020-06-01")

    assert entity.snapshot(snapshot_date) == {
        "a": "b",
        "c": "d",
        "entity": 2,
        "slug": "/one",
        "entry-date": "2020-01-01",
    }


def test_change_history():
    entry_1 = Entry(
        {"a": "b", "entity": 1, "slug": "/one", "entry-date": "2019-01-01"}, "abc123", 1
    )
    entry_2 = Entry(
        {"a": "b", "entity": 1, "slug": "/one", "entry-date": "2020-01-01"},
        "def456",
        10,
    )
    entry_3 = Entry(
        {"a": "e", "entity": 1, "slug": "/one", "entry-date": "2021-01-01"},
        "xzy789",
        99,
    )
    entries = [entry_1, entry_2, entry_3]
    entity = Entity(entries, "conservation-area")

    history = entity.change_history()

    assert len(history) == 2
    assert history == [entry_3, entry_1]


def test_all_fields():
    entry_1 = Entry(
        {"a": "b", "entity": 1, "slug": "/one", "entry-date": "2019-01-01"}, "abc123", 1
    )
    entry_2 = Entry(
        {"a": "b", "entity": 1, "slug": "/one", "entry-date": "2020-01-01"},
        "def456",
        10,
    )
    entry_3 = Entry(
        {"c": "e", "entity": 1, "slug": "/one", "entry-date": "2021-01-01"},
        "xzy789",
        99,
    )
    entries = [entry_1, entry_2, entry_3]
    entity = Entity(entries, "conservation-area")

    fields = entity.all_fields()

    assert len(fields) == 2
    assert fields == {"a", "c"}
