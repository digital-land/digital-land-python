import os
import csv


class Pipeline:
    column = {}

    def __init__(self, pipeline, path):
        self.pipeline = pipeline
        self.load(path)

    def load(self, path):
        reader = csv.DictReader(open(os.path.join(path, "column.csv")))
        for row in reader:
            if row["pipeline"] != self.pipeline:
                continue

            column = self.column.setdefault(row["resource"], {})
            column[row["pattern"]] = row["value"]

    def column_typos(self, resource=""):
        if not resource:
            return self.column.get("", {})

        return {**self.column.get(resource, {}), **self.column.get("", {})}
