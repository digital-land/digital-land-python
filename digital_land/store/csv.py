# CSV store, a set of CSV files of entries in a directory

import os
import csv
import logging
from pathlib import Path
from .memory import MemoryStore


class CSVStore(MemoryStore):
    def csv_path(store, directory=""):
        return Path(directory) / (store.schema.name + ".csv")

    def load_csv(self, path=None, directory=""):
        path = path or self.csv_path(directory)
        logging.debug("loading %s" % (path))
        reader = csv.DictReader(open(path, newline=""))
        for row in reader:
            self.add_entry(row)

    def load(self, *args, **kwargs):
        self.load_csv(*args, **kwargs)

    def save_csv(self, path=None, directory="", entries=None):
        path = path or self.csv_path(directory)

        if entries is None:
            entries = self.entries

        os.makedirs(os.path.dirname(path), exist_ok=True)
        logging.error("saving %s" % (path))
        f = open(path, "w", newline="")
        writer = csv.DictWriter(
            f, fieldnames=self.schema.fieldnames, extrasaction="ignore"
        )
        writer.writeheader()
        for entry in entries:
            writer.writerow(entry)

    def save(self, *args, **kwargs):
        self.save_csv(*args, **kwargs)
