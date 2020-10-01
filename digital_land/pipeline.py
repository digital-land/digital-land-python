import csv
import os
import re


class Pipeline:
    def __init__(self, path, name):
        self.name = name
        self.path = path
        self.schema = []
        self.column = {}
        self.skip_pattern = {}
        self.patch = {}
        self.default = {}
        self.concat = {}
        self.transform = {}
        self.load_pipeline()
        self.load_column()
        self.load_skip_patterns()
        self.load_patch()
        self.load_default()
        self.load_concat()
        self.load_transform()

    def _row_reader(self, filename):
        # read a file from the pipeline path, ignore if missing
        # and filter out rows not relevant to this pipeline

        file = os.path.join(self.path, filename)
        if not os.path.isfile(file):
            return []
        reader = csv.DictReader(open(file))
        for row in reader:
            if row["pipeline"] != self.name:
                continue
            yield row

    def load_pipeline(self):
        reader = self._row_reader("pipeline.csv")
        for row in reader:
            self.schema = row["schema"]

    def load_column(self):
        reader = self._row_reader("column.csv")
        for row in reader:
            column = self.column.setdefault(row["resource"], {})
            column[self.normalise(row["pattern"])] = row["value"]

    def load_skip_patterns(self):
        reader = self._row_reader("skip.csv")
        for row in reader:
            pattern = self.skip_pattern.setdefault(row["resource"], [])
            pattern.append(row["pattern"])

    def load_patch(self):
        reader = self._row_reader("patch.csv")
        for row in reader:
            resource_patch = self.patch.setdefault(row["resource"], {})
            field_patch = resource_patch.setdefault(row["field"], {})
            field_patch[row["pattern"]] = row["value"]

    def load_default(self):
        reader = self._row_reader("default.csv")
        for row in reader:
            resource_default = self.default.setdefault(row["resource"], {})
            field_default = resource_default.setdefault(row["field"], [])
            field_default.append(row["default-field"])

    def load_concat(self):
        reader = self._row_reader("concat.csv")
        for row in reader:
            resource_concat = self.concat.setdefault(row["resource"], {})
            resource_concat[row["field"]] = {
                "fields": row["fields"].split(";"),
                "separator": row["separator"],
            }

    def load_transform(self):
        reader = self._row_reader("transform.csv")
        for row in reader:
            if row["replacement-field"] == "":
                continue

            if row["replacement-field"] in self.transform:
                raise ValueError(
                    "replacement-field %s has more than one transform entry"
                    % row["replacement-field"]
                )

            self.transform[row["replacement-field"]] = row["field"]

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

    def concatenations(self, resource=None):
        general_concat = self.concat.get("", {})

        if not resource:
            return general_concat

        resource_concat = self.concat.get(resource, {})

        result = {}
        result.update(general_concat)
        result.update(resource_concat)
        return result

    def transformations(self):
        return self.transform

    def conversions(self):
        return {}  # TODO

    normalise_pattern = re.compile(r"[^a-z0-9-]")

    def normalise(self, name):
        return re.sub(self.normalise_pattern, "", name.lower())
