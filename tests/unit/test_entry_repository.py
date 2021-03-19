from digital_land.model.entry import Entry
from digital_land.model.fact import Fact
from digital_land.repository.entry_repository import EntryRepository


def test_add():
    repo = EntryRepository(":memory:", create=True)
    entry = Entry(
        {"a": "b", "slug": "/slug/one", "entry-date": "2021-03-19"}, "abc123", 1
    )

    repo.add(entry)

    result = repo.find_by_entity("/slug/one")

    assert len(result) == 1
    assert result.pop() == entry


def test_find_by_entity():
    repo = EntryRepository(":memory:", create=True)
    entry_1 = Entry(
        {"a": "b", "slug": "/slug/one", "entry-date": "2021-03-19"}, "abc123", 1
    )
    entry_2 = Entry(
        {"a": "b", "slug": "/slug/one", "entry-date": "2021-03-19"}, "abc123", 2
    )
    entry_3 = Entry(
        {"c": "d", "slug": "/slug/two", "entry-date": "2021-03-19"}, "abc123", 3
    )
    entries = set([entry_1, entry_2, entry_3])

    for entry in entries:
        repo.add(entry)

    result = repo.find_by_entity("/slug/one")

    assert len(result) == 2
    assert entry_1 in result
    assert entry_2 in result
    assert entry_3 not in result


def test_find_by_fact():
    repo = EntryRepository(":memory:", create=True)
    entry_1 = Entry(
        {"a": "b", "slug": "/slug/one", "entry-date": "2021-03-19"}, "abc123", 1
    )
    entry_2 = Entry(
        {"a": "c", "slug": "/slug/one", "entry-date": "2021-03-19"}, "abc123", 2
    )
    entry_3 = Entry(
        {"c": "d", "slug": "/slug/two", "entry-date": "2021-03-19"}, "abc123", 3
    )
    entries = set([entry_1, entry_2, entry_3])

    for entry in entries:
        repo.add(entry)

    fact = Fact("/slug/one", "a", "b")

    result = repo.find_by_fact(fact)

    assert len(result) == 1
    assert result == set([entry_1])
