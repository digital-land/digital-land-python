import os
import csv


class Pipeline:
    def __init__(self, path, name):
        self.name = name
        self.schema = []
        self.column = {}
        self.skip_pattern = {}
        self.patch = {}
        self.default = {}
        self.load_pipeline(path)
        self.load_column(path)
        self.load_skip_patterns(path)
        self.load_patch(path)
        self.load_default(path)

    def load_pipeline(self, path):
        reader = csv.DictReader(open(os.path.join(path, "pipeline.csv")))
        for row in reader:
            if row["pipeline"] != self.name:
                continue

            self.schema = row["schema"]

    def load_column(self, path):
        reader = csv.DictReader(open(os.path.join(path, "column.csv")))
        for row in reader:
            if row["pipeline"] != self.name:
                continue

            column = self.column.setdefault(row["resource"], {})
            column[row["pattern"]] = row["value"]

    def load_skip_patterns(self, path):
        reader = csv.DictReader(open(os.path.join(path, "skip.csv")))
        for row in reader:
            if row["pipeline"] != self.name:
                continue

            pattern = self.skip_pattern.setdefault(row["resource"], [])
            pattern.append(row["pattern"])

    def load_patch(self, path):
        reader = csv.DictReader(open(os.path.join(path, "patch.csv")))
        for row in reader:
            if row["pipeline"] != self.name:
                continue

            resource_patch = self.patch.setdefault(row["resource"], {})
            field_patch = resource_patch.setdefault(row["field"], {})
            field_patch[row["pattern"]] = row["value"]

    def load_default(self, path):
        reader = csv.DictReader(open(os.path.join(path, "default.csv")))
        for row in reader:
            if row["pipeline"] != self.name:
                continue

            resource_default = self.default.setdefault(row["resource"], {})
            field_default = resource_default.setdefault(row["field"], [])
            field_default.append(row["default-field"])

    def columns(self, resource=""):
        if not resource:
            return self.column.get("", {})

        return {**self.column.get(resource, {}), **self.column.get("", {})}

    def skip_patterns(self, resource=""):
        if not resource:
            return self.skip_pattern.get("", {})

        return self.skip_pattern.get(resource, []) + self.skip_pattern.get("", [])

    def patches(self, resource=""):
        general_patch = self.patch.get("", {})
        if not resource:
            return general_patch

        resource_patch = self.patch.get(resource, {})

        result = {}
        for field, patch in resource_patch.items():
            result[field] = {**patch, **general_patch.pop(field, {})}

        # Merge any remaining general defaults into the result
        result.update(general_patch)

        return result

    def default_fieldnames(self, resource=None):
        general_default = self.default.get("", {})
        if not resource:
            return general_default

        resource_default = self.default.get(resource, {})

        result = {}
        for field, default in resource_default.items():
            result[field] = default + general_default.pop(field, [])

        # Merge any remaining general defaults into the result
        result.update(general_default)

        return result
