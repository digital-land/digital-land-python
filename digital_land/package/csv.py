import csv
import logging
import os
from .package import Package

logger = logging.getLogger(__name__)


class CsvPackage(Package):
    def __init__(self, *args, **kwargs):
        self.suffix = ".csv"
        self.join_tables = {}
        self.fields = {}
        super().__init__(*args, **kwargs)

    def write(self, fieldnames, rows):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
