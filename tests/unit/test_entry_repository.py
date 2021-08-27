from digital_land.model.entry import Entry
from digital_land.model.fact import Fact
from digital_land.repository.entry_repository import EntryRepository


def test_add():
    repo = EntryRepository(":memory:", create=True)
    entry = Entry(
        {
            "a": "b",
            "entity": 1,
            "slug": "/slug/one",
            "entry-date": "2021-03-19",
            "start-date": "2020-01-01",
        },
        "abc123",
        1,
    )

    repo.add(entry)

    result = repo.find_by_slug("/slug/one")

    assert len(result) == 1
    assert result.pop() == entry


def test_find_by_slug():
    repo = EntryRepository(":memory:", create=True)
    entry_1 = Entry(
        {"a": "b", "entity": 1, "slug": "/slug/one", "entry-date": "2021-03-19"},
        "abc123",
        1,
    )
    entry_2 = Entry(
        {"a": "b", "entity": 1, "slug": "/slug/one", "entry-date": "2021-03-19"},
        "abc123",
        2,
    )
    entry_3 = Entry(
        {"c": "d", "entity": 1, "slug": "/slug/two", "entry-date": "2021-03-19"},
        "abc123",
        3,
    )
    entries = set([entry_1, entry_2, entry_3])

    for entry in entries:
        repo.add(entry)

    result = repo.find_by_slug("/slug/one")

    assert len(result) == 2
    assert entry_1 in result
    assert entry_2 in result
    assert entry_3 not in result


def test_find_by_entity():
    repo = EntryRepository(":memory:", create=True)
    entry_1 = Entry(
        {"a": "b", "entity": 1, "slug": "/slug/one", "entry-date": "2021-03-19"},
        "abc123",
        1,
    )
    entry_2 = Entry(
        {"c": "d", "entity": 1, "slug": "/slug/two", "entry-date": "2021-03-19"},
        "abc123",
        3,
    )
    entry_3 = Entry(
        {"e": "f", "entity": 2, "slug": "/slug/three", "entry-date": "2021-03-19"},
        "abc123",
        2,
    )

    entries = set([entry_1, entry_2, entry_3])

    for entry in entries:
        repo.add(entry)

    result = repo.find_by_entity(1)

    assert len(result) == 2
    assert entry_1 in result
    assert entry_2 in result
    assert entry_3 not in result


def test_slug_list():
    repo = EntryRepository(":memory:", create=True)
    entry_1 = Entry(
        {"a": "b", "entity": 1, "slug": "/c", "entry-date": "2021-03-19"}, "abc123", 1
    )
    entry_2 = Entry(
        {"a": "c", "entity": 1, "slug": "/a", "entry-date": "2021-03-19"}, "abc123", 2
    )
    entry_3 = Entry(
        {"c": "d", "entity": 2, "slug": "/b", "entry-date": "2021-03-19"}, "abc123", 3
    )
    entries = set([entry_1, entry_2, entry_3])

    for entry in entries:
        repo.add(entry)

    result = repo.list_slugs()

    assert len(result) == 3
    assert result == ["/a", "/b", "/c"]


def test_entity_list():
    repo = EntryRepository(":memory:", create=True)
    entry_1 = Entry(
        {"a": "b", "entity": 1, "slug": "/c", "entry-date": "2021-03-19"}, "abc123", 1
    )
    entry_2 = Entry(
        {"a": "c", "entity": 1, "slug": "/a", "entry-date": "2021-03-19"}, "abc123", 2
    )
    entry_3 = Entry(
        {"c": "d", "entity": 2, "slug": "/b", "entry-date": "2021-03-19"}, "abc123", 3
    )
    entries = set([entry_1, entry_2, entry_3])

    for entry in entries:
        repo.add(entry)

    result = repo.list_entities()

    assert len(result) == 2
    assert result == [1, 2]


def test_attribute_list():
    repo = EntryRepository(":memory:", create=True)
    entry_1 = Entry({"a": "b", "slug": "/c", "entry-date": "2021-03-19"}, "abc123", 1)
    entry_2 = Entry({"a": "c", "slug": "/a", "entry-date": "2021-03-19"}, "abc123", 2)
    entry_3 = Entry({"c": "d", "slug": "/b", "entry-date": "2021-03-19"}, "abc123", 3)
    entries = set([entry_1, entry_2, entry_3])

    for entry in entries:
        repo.add(entry)

    result = repo.list_attributes()

    assert len(result) == 2
    assert result == {"a", "c"}


def test_find_by_fact():
    repo = EntryRepository(":memory:", create=True)
    entry_1 = Entry(
        {"a": "b", "entity": 1, "slug": "/slug/one", "entry-date": "2021-03-19"},
        "abc123",
        1,
    )
    entry_2 = Entry(
        {"a": "c", "entity": 1, "slug": "/slug/one", "entry-date": "2021-03-19"},
        "abc123",
        2,
    )
    entry_3 = Entry(
        {"c": "d", "entity": 1, "slug": "/slug/two", "entry-date": "2021-03-19"},
        "abc123",
        3,
    )
    entries = set([entry_1, entry_2, entry_3])

    for entry in entries:
        repo.add(entry)

    fact = Fact(1, "/slug/one", "a", "b")

    result = repo.find_by_fact(fact)

    assert len(result) == 1
    assert result == set([entry_1])
