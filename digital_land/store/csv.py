# CSV store, a set of CSV files of entries in a directory

import csv
from pathlib import Path
from .memory import MemoryStore


class CSVStore(MemoryStore):
    def __init__(self, register, fieldnames=[], key=None, directory=".", path=None):
        super().__init__(register, fieldnames=fieldnames, key=key)
        self.directory = directory
        self.path = path or Path(self.directory) / self.register + ".csv"

    def load(self, path=None):
        path = path or self.path
        reader = csv.DictReader(open(path, newline=""))
        if not self.fieldnames:
            self.fieldnames = reader.fieldnames
        for row in reader:
            self.add_entry(row)

    def save(self, path=None):
        path = path or self.path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        f = open(path, "w", newline="")
        writer = csv.DictWriter(f, fieldnames=self.fieldnames, extrasaction="ignore")
        writer.writeheader()
        for entry in self.entries:
            writer.writerow(entry.item)
