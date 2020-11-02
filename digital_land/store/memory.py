# a register in memory store


class MemoryStore:
    def __init__(self, register, fieldnames=[], key=None, directory=".", path=None):
        self.register = register
        self.key = key or self.register
        self.fieldnames = fieldnames
        self.entries = []
        self.records = {}

    def add_entry(self, item):
        self.entries.append(item)
        value = item[self.key]
        self.records.setdefault(value, [])
        self.records[value].append(len(self.entries) - 1)

    def get_record(self, key_value):
        if key_value not in self.records:
            raise KeyError("missing record")
        return [self.entries[i] for i in self.records[key_value]]
