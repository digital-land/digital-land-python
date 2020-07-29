import re
import os
import csv


class Pipeline:
    def __init__(self, pipeline, path):
        self.column = {}
        self.fieldnames = []
        self.pipeline = pipeline
        self.load(path)

    def load(self, path):
        reader = csv.DictReader(open(os.path.join(path, "column.csv")))
        for row in reader:
            if row["pipeline"] != self.pipeline:
                continue

            column = self.column.setdefault(row["resource"], {})
            column[row["pattern"]] = row["value"]

            if row["value"] not in self.fieldnames:
                self.fieldnames.append(row["value"])

    def columns(self, resource=""):
        if not resource:
            return self.column.get("", {})

        return {**self.column.get(resource, {}), **self.column.get("", {})}
