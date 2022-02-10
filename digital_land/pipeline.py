import os
import csv
import functools
import importlib.util
import logging
from .phase.map import normalise
from .phase.lookup import key as lookup_key


def chain_phases(phases):
    def add(f, g):
        return lambda x: g.process(f(x))

    return functools.reduce(add, phases, lambda phase: phase)


def run_pipeline(*args):
    logging.debug(f"run_pipeline {args}")
    chain = chain_phases(args)

    stream = chain(None)
    for row in stream:
        pass


class Pipeline:
    def __init__(self, path, dataset):
        self.dataset = dataset
        self.name = dataset
        self.path = path
        self.column = {}
        self.filter = {}
        self.skip_pattern = {}
        self.patch = {}
        self.default = {}
        self.concat = {}
        self.migrate = {}
        self.lookup = {}
        self.load_column()
        self.load_skip_patterns()
        self.load_patch()
        self.load_default()
        self.load_concat()
        self.load_migrate()
        self.load_lookup()
        self.load_filter()

    def _reader(self, filename):
        # read a file from the pipeline path, ignore if missing
        file = os.path.join(self.path, filename)
        if not os.path.isfile(file):
            return []
        return csv.DictReader(open(file))

    def _row_reader(self, filename):
        reader = self._reader(filename)

        # filter out rows not relevant to this pipeline
        for row in reader:
            if row["pipeline"] and row["pipeline"] != self.name:
                continue
            yield row

    @property
    def schema(self):
        raise NotImplementedError()

    def load_column(self):
        reader = self._row_reader("column.csv")
        for row in reader:
            resource_column = self.column.setdefault(row["resource"], {})

            # migrate column.csv
            row["dataset"] = row.get("dataset", "") or row["pipeline"]
            row["column"] = row.get("column", "") or row["pattern"]
            row["field"] = row.get("field", "") or row["value"]

            resource_column[normalise(row["column"])] = row["field"]

    def load_filter(self):
        reader = self._row_reader("filter.csv")
        for row in reader:
            filter = self.filter.setdefault(row["resource"], {})
            filter[row["field"]] = row["pattern"]

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

    def load_migrate(self):
        reader = self._row_reader("transform.csv")
        for row in reader:
            if row["replacement-field"] == "":
                continue

            if row["replacement-field"] in self.migrate:
                raise ValueError(
                    "replacement-field %s has more than one entry"
                    % row["replacement-field"]
                )

            self.migrate[row["replacement-field"]] = row["field"]

    def load_lookup(self):
        reader = self._reader("lookup.csv")
        for row in reader:
            resource_lookup = self.lookup.setdefault(row["resource"], {})

            # migrate old lookup.csv files
            dataset = row.get("dataset", "") or row.get("pipeline", "")
            prefix = row.get("prefix", "") or dataset
            reference = row.get("reference", "") or row.get("value", "")
            line_number = row.get("line-number", "") or row.get("row-number", "")
            organisation = row.get("organisation", "")

            # composite key, ordered by specificity
            resource_lookup[
                lookup_key(
                    line_number=line_number,
                    organisation=organisation,
                    prefix=prefix,
                    reference=reference,
                )
            ] = row["entity"]

    def filters(self, resource=""):
        general_filters = self.filter.get("", {})
        if not resource:
            return general_filters

        resource_filters = self.filter.get(resource, {})
        result = {}
        result.update(general_filters)
        result.update(resource_filters)

        return result

    def columns(self, resource=""):
        general_columns = self.column.get("", {})
        if not resource:
            return general_columns

        resource_columns = self.column.get(resource, {})

        result = resource_columns
        for key in general_columns:
            if key in result:
                continue
            result[key] = general_columns[key]
        return result

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

    def migrations(self):
        return self.migrate

    def conversions(self):
        return {}  # TODO

    def lookups(self, resource=None):
        general_lookup = self.lookup.get("", {})

        if not resource:
            return general_lookup

        resource_lookup = self.lookup.get(resource, {})

        result = {}
        result.update(general_lookup)
        result.update(resource_lookup)
        return result

    def get_pipeline_callback(self):
        file = os.path.join(self.path, "pipeline-callback.py")
        spec = importlib.util.spec_from_file_location("pipeline-callback.py", file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.PipelineCallback
