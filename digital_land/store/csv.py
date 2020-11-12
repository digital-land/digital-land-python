# CSV store, a set of CSV files of entries in a directory

import os
import csv
import logging
from pathlib import Path
from .memory import MemoryStore


class CSVStore(MemoryStore):
    def path(self, directory=""):
        return Path(directory) / (self.schema.name + ".csv")

    def load(self, path=None, directory=""):
        path = path or self.path(directory)
        logging.debug("loading %s" % (path))
        reader = csv.DictReader(open(path, newline=""))
        for row in reader:
            self.add_entry(row)

    def save(self, path=None, directory="", entries=None):
        path = path or self.path(directory)
        fieldnames = self.schema.fieldnames

        if entries is None:
            entries = self.entries

        os.makedirs(os.path.dirname(path), exist_ok=True)
        logging.debug("saving %s" % (path))
        f = open(path, "w", newline="")
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for entry in entries:
            writer.writerow(entry)
