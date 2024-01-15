import os
import csv
import functools
import importlib.util
import logging
from pathlib import Path

from .phase.map import normalise
from .phase.lookup import key as lookup_key
from .schema import Schema


def chain_phases(phases):
    def add(f, g):
        return lambda x: g.process(f(x))

    return functools.reduce(add, phases, lambda phase: phase)


def run_pipeline(*args):
    logging.debug(f"run_pipeline {args}")
    chain = chain_phases([arg for arg in args if arg])

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
        self.default_field = {}
        self.default_value = {}
        self.combine_field = {}
        self.concat = {}
        self.migrate = {}
        self.lookup = {}

        self.load_column()
        self.load_skip_patterns()
        self.load_patch()
        self.load_default_fields()
        self.load_default_values()
        self.load_concat()
        self.load_combine_fields()
        self.load_migrate()
        self.load_lookup()
        self.load_filter()

    def file_reader(self, filename):
        # read a file from the pipeline path, ignore if missing
        path = os.path.join(self.path, filename)
        if not os.path.isfile(path):
            return []
        logging.debug(f"load {path}")
        return csv.DictReader(open(path))

    def reader(self, filename):
        for row in self.file_reader(filename):
            row["dataset"] = row.get("dataset", "") or row["pipeline"]
            if row["dataset"] and row["dataset"] != self.name:
                continue
            yield row

    def load_column(self):
        for row in self.reader("column.csv"):
            resource = row.get("resource", "")
            endpoint = row.get("endpoint", "")

            if resource:
                record = self.column.setdefault(resource, {})
            elif endpoint:
                record = self.column.setdefault(endpoint, {})
            else:
                record = self.column.setdefault("", {})

            # migrate column.csv
            row["column"] = row.get("column", "") or row["pattern"]
            row["field"] = row.get("field", "") or row["value"]

            record[normalise(row["column"])] = row["field"]

    def load_filter(self):
        for row in self.reader("filter.csv"):
            record = self.filter.setdefault(row["resource"], {})
            record[row["field"]] = row["pattern"]

    def load_skip_patterns(self):
        for row in self.reader("skip.csv"):
            record = self.skip_pattern.setdefault(row["resource"], [])
            record.append(row["pattern"])

    def load_patch(self):
        for row in self.reader("patch.csv"):
            record = self.patch.setdefault(row["resource"], {})
            record = record.setdefault(row["field"], {})
            record[row["pattern"]] = row["value"]

    def load_default_fields(self):
        # TBD: rename default-field.csv
        for row in self.reader("default.csv"):
            record = self.default_field.setdefault(row.get("resource", ""), {})
            record[row["field"]] = row["default-field"]

    def load_default_values(self):
        for row in self.reader("default-value.csv"):
            record = self.default_value.setdefault(row.get("endpoint", ""), {})
            record[row["field"]] = row["value"]

    def load_combine_fields(self):
        for row in self.reader("combine.csv"):
            record = self.combine_field.setdefault(row.get("endpoint", ""), {})
            record[row["field"]] = row["separator"]

    def load_concat(self):
        for row in self.reader("concat.csv"):
            resource = row.get("resource", "")
            endpoint = row.get("endpoint", "")

            if resource:
                record = self.concat.setdefault(resource, {})
            elif endpoint:
                record = self.concat.setdefault(endpoint, {})
            else:
                record = self.concat.setdefault("", {})

            # record = self.concat.setdefault(row["resource"], {})
            record[row["field"]] = {
                "fields": row["fields"].split(";"),
                "separator": row["separator"],
            }

    # TBD: remove this table, should come from specification replacement-field
    def load_migrate(self):
        for row in self.reader("transform.csv"):
            if row["replacement-field"] == "":
                continue

            if row["replacement-field"] in self.migrate:
                raise ValueError(
                    "replacement-field %s has more than one entry"
                    % row["replacement-field"]
                )

            self.migrate[row["replacement-field"]] = row["field"]

    def load_lookup(self):
        for row in self.file_reader("lookup.csv"):
            # migrate old lookup.csv files
            entry_number = row.get("entry-number", "")
            prefix = (
                row.get("prefix", "")
                or row.get("dataset", "")
                or row.get("pipeline", "")
            )
            reference = row.get("reference", "") or row.get("value", "")

            # composite key, ordered by specificity
            resource_lookup = self.lookup.setdefault(row.get("resource", ""), {})
            resource_lookup[
                lookup_key(
                    entry_number=entry_number,
                    prefix=prefix,
                    reference=reference,
                )
            ] = row["entity"]

            organisation = row.get("organisation", "")
            resource_lookup[
                lookup_key(
                    prefix=prefix,
                    reference=reference,
                    organisation=organisation,
                )
            ] = row["entity"]

    def filters(self, resource=""):
        d = self.filter.get("", {})
        if resource:
            d.update(self.filter.get(resource, {}))
        return d

    def columns(self, resource="", endpoints=[]):
        general_columns = self.column.get("", {})
        if not resource:
            return general_columns

        resource_columns = self.column.get(resource, {})
        endpoint_columns = {}
        for endpoint in endpoints:
            endpoint_columns = {**endpoint_columns, **self.column.get(endpoint, {})}

        result = {**endpoint_columns, **resource_columns}

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

    def default_fields(self, resource=None):
        config = self.default_field
        d = config.get("", {})
        for key, value in config.get(resource, {}).items():
            d[key] = value
        return d

    def default_values(self, endpoints=None):
        if endpoints is None:
            endpoints = []
        config = self.default_value
        d = config.get("", {})
        for endpoint in endpoints:
            for key, value in config.get(endpoint, {}).items():
                d[key] = value
        return d

    def combine_fields(self, endpoints=None):
        if endpoints is None:
            endpoints = []
        config = self.combine_field
        d = config.get("", {})
        for endpoint in endpoints:
            for key, value in config.get(endpoint, {}).items():
                d[key] = value
        return d

    def concatenations(self, resource=None, endpoints=[]):
        result = self.concat.get("", {})
        if resource:
            result.update(self.concat.get(resource, {}))

        for endpoint in endpoints:
            result.update(self.concat.get(endpoint, {}))

        return result

    def migrations(self):
        return self.migrate

    def lookups(self, resource=None):
        d = self.lookup.get("", {})
        if resource:
            d.update(self.lookup.get(resource, {}))
        return d

    def get_pipeline_callback(self):
        file = os.path.join(self.path, "pipeline-callback.py")
        spec = importlib.util.spec_from_file_location("pipeline-callback.py", file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.PipelineCallback

    @staticmethod
    def compose(phases):
        def add(f, g):
            return lambda x: g.process(f(x))

        return functools.reduce(add, phases, lambda phase: phase)

    def run(self, input_path, phases):
        logging.debug(f"running {input_path} through {phases}")
        chain = self.compose(phases)
        for row in chain(input_path):
            pass


class EntityNumGen:
    def __init__(self, entity_num_state: dict = None):
        if not entity_num_state:
            entity_num_state = {
                "range_min": 0,
                "range_max": 100,
                "current": 0,
            }

        self.state = entity_num_state

    def next(self):
        current = self.state["current"]
        new_current = current + 1

        if new_current > int(self.state["range_max"]):
            new_current = int(self.state["range_min"])

        if new_current < int(self.state["range_min"]):
            new_current = int(self.state["range_min"])

        self.state["current"] = new_current

        return new_current


class Lookups:
    def __init__(self, directory=None) -> None:
        self.directory = directory or "pipeline"
        self.lookups_path = Path(directory) / "lookup.csv"
        self.entries = []
        self.schema = Schema("lookup")
        self.entity_num_gen = EntityNumGen()

    def add_entry(self, entry, is_new_entry=True):
        """
        is_new_entry is an addition to allow for backward compatibility.
        Older lookups may not be valid in accordance with the current
        minimal column requirements
        :param entry:
        :param is_new_entry:
        :return:
        """
        if is_new_entry:
            if not self.validate_entry(entry):
                return

        self.entries.append(entry)

    def load_csv(self, lookups_path=None):
        """
        load in lookups as df, not when we process pipeline but useful for other analysis
        """
        lookups_path = lookups_path or self.lookups_path
        reader = csv.DictReader(open(lookups_path, newline=""))
        extra_fields = set(reader.fieldnames) - set(self.schema.fieldnames)

        if len(extra_fields):
            raise RuntimeError(f"{len(extra_fields)} extra fields founds in lookup.csv ({','.join(list(extra_fields))})")

        for row in reader:
            self.add_entry(row, is_new_entry=False)

    def get_max_entity(self, prefix) -> int:
        if len(self.entries) == 0:
            return 0
        if not prefix:
            return 0

        try:
            ret_val = max(
                [
                    int(entry["entity"])
                    for entry in self.entries
                    if (entry["prefix"] == prefix) and (entry.get("entity", None))
                ]
            )
            return ret_val
        except ValueError:
            return 0

    def save_csv(self, lookups_path=None, entries=None):
        path = lookups_path or self.lookups_path

        if entries is None:
            entries = self.entries

        os.makedirs(os.path.dirname(path), exist_ok=True)
        logging.debug("saving %s" % (path))
        f = open(path, "w", newline="")
        writer = csv.DictWriter(
            f, fieldnames=self.schema.fieldnames, extrasaction="ignore"
        )
        writer.writeheader()
        for idx, entry in enumerate(entries):
            if not entry:
                continue
            else:
                if not entry.get("entity"):
                    entry["entity"] = self.entity_num_gen.next()
                writer.writerow(entry)

    # @staticmethod
    def validate_entry(self, entry) -> bool:
        # ensures minimum expected fields exist and are not empty strings
        expected_fields = ["prefix", "organisation", "reference"]
        for field in expected_fields:
            if not entry.get(field, ""):
                raise ValueError(f"ERROR: expected {field} not found in lookup entry")

        if len(self.entries) > 0:
            # check entry does not already exist
            existing_entries = len(
                [
                    1
                    for item in self.entries
                    if item["prefix"] == entry["prefix"]
                    and item["organisation"] == entry["organisation"]
                    and item["reference"] == entry["reference"]
                ]
            )

            if existing_entries > 0:
                # print(f">>> ERROR: lookup already exists - {entry['organisation']} {entry['reference']}")
                return False

        return True

    # I'm not sure we need this class or if we do it should be an iterator then can be used to iterate through entity numbers by dataset
