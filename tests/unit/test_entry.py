import pytest

from digital_land.model.entry import Entry
from digital_land.model.fact import Fact


def test_init():
    data = {"a": "b", "slug": "/slug/abc"}
    resource = "abc123"
    line_num = 1

    entry = Entry(data, resource, line_num)

    assert entry.data == data
    assert entry.resource == resource
    assert entry.line_num == line_num
    assert entry.slug == "/slug/abc"


def test_missing_slug_exception():
    data = {"a": "b"}
    resource = "abc123"
    line_num = 1

    with pytest.raises(ValueError, match="^Entry missing slug field$"):
        Entry(data, resource, line_num)


def test_to_facts():
    slug = "/slug/abc"
    data = {"a": "b", "c": "d", "e": "f", "slug": slug}
    entry = Entry(data, "abc123", 1)

    facts = entry.facts

    assert facts == set(
        [Fact(slug, "a", "b"), Fact(slug, "c", "d"), Fact(slug, "e", "f")]
    )


def test_from_facts():
    slug = "/slug/abc"
    resource = "abc123"
    line_num = 1
    facts = set([Fact(slug, "a", "b"), Fact(slug, "c", "d"), Fact(slug, "e", "f")])

    entry = Entry.from_facts(slug, facts, resource, line_num)

    assert entry.slug == slug
    assert entry.resource == resource
    assert entry.line_num == line_num
    assert entry.data == {"a": "b", "c": "d", "e": "f", "slug": slug}


def test_equality():
    entry_1 = Entry({"a": "b", "c": "d", "e": "f", "slug": "/slug/abc"}, "abc123", 1)
    entry_2 = Entry({"c": "d", "e": "f", "a": "b", "slug": "/slug/abc"}, "abc123", 1)
    entry_3 = Entry({"e": "f", "a": "b", "slug": "/slug/abc"}, "abc123", 1)

    assert entry_1 == entry_2  # ordering of the dict items is ignored
    assert entry_1 != entry_3
