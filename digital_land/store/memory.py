# a register in-memory store

from .store import Store


class MemoryStore(Store):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.entries = []
        self.records = {}

    def add_entry(self, item):
        self.entries.append(item)
        value = item[self.schema.key]
        self.records.setdefault(value, [])
        self.records[value].append(item)
