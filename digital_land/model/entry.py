from datetime import date
from typing import Set

from ..register import Item
from .fact import Fact

SKIP_FACT_ATTRIBUTES = ["slug", "entry-date"]


class Entry(Item):
    def __init__(self, data: Item, resource: str, line_num: int = None):
        self.data = data
        self.resource = resource
        self.line_num = line_num

        if not data.get("slug", None):
            raise ValueError("Entry missing slug field")

        self.slug = data.pop("slug")

        if not data.get("entry-date", None):
            raise ValueError("Entry missing entry-date")

        self.entry_date = date.fromisoformat(data.pop("entry-date"))

    def __hash__(self):
        return hash((self.resource, self.line_num, frozenset(self.data.items())))

    @classmethod
    def from_facts(
        cls,
        entity: str,
        facts: Set[Fact],
        resource: str,
        line_num: int,
        entry_date: str,
    ):
        "construct an Entry from a set of Facts"

        data = {fact.attribute: fact.value for fact in facts}
        data["slug"] = entity
        data["entry-date"] = entry_date
        return Entry(data, resource, line_num)

    @property
    def facts(self):
        "returns a set of Fact objects representing this entry"

        return {
            Fact(self.slug, attribute, value)
            for attribute, value in self.data.items()
            if attribute not in SKIP_FACT_ATTRIBUTES
        }

    def __repr__(self):
        return f"resource: {self.resource}, line: {self.line_num}, data: {self.data}"

    def __gt__(self, other):
        return self.entry_date > other.entry_date
