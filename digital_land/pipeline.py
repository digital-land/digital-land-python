import os
import csv


class Pipeline:
    def __init__(self, path):
        self.pipeline = {}
        self.column = {}
        self.skip_pattern = {}
        self.patch = {}
        self.load_pipeline(path)
        self.load_column(path)
        self.load_skip_patterns(path)
        self.load_patch(path)

    def load_pipeline(self, path):
        reader = csv.DictReader(open(os.path.join(path, "pipeline.csv")))
        for row in reader:
            self.pipeline[row["pipeline"]] = row["schema"]

    def load_column(self, path):
        reader = csv.DictReader(open(os.path.join(path, "column.csv")))
        for row in reader:
            pipeline_column = self.column.setdefault(row["pipeline"], {})
            column = pipeline_column.setdefault(row["resource"], {})
            column[row["pattern"]] = row["value"]

    def load_skip_patterns(self, path):
        reader = csv.DictReader(open(os.path.join(path, "skip.csv")))
        for row in reader:
            pipeline_skip_pattern = self.skip_pattern.setdefault(row["pipeline"], {})
            pattern = pipeline_skip_pattern.setdefault(row["resource"], [])
            pattern.append(row["pattern"])

    def load_patch(self, path):
        reader = csv.DictReader(open(os.path.join(path, "patch.csv")))
        for row in reader:
            pipeline_patch = self.patch.setdefault(row["pipeline"], {})
            resource_patch = pipeline_patch.setdefault(row["resource"], {})
            field_patch = resource_patch.setdefault(row["field"], [])
            field_patch.append((row["pattern"], row["value"]))

    def columns(self, pipeline, resource=""):
        column = self.column[pipeline]

        if not resource:
            return column.get("", {})

        return {**column.get(resource, {}), **column.get("", {})}

    def skip_patterns(self, pipeline, resource=""):
        pattern = self.skip_pattern[pipeline]

        if not resource:
            return pattern.get("", {})

        return {**pattern.get(resource, {}), **pattern.get("", {})}

    def patches(self, pipeline, resource=""):
        patch = self.patch[pipeline]

        if not resource:
            return patch.get("", {})

        return {**patch.get(resource, {}), **patch.get("", {})}
