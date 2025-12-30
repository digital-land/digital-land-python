import os
import csv
import functools
import importlib.util
import logging
from pathlib import Path

from digital_land.phase.map import normalise
from digital_land.phase.lookup import key as lookup_key
from digital_land.schema import Schema

from digital_land.phase.combine import FactCombinePhase
from digital_land.phase.concat import ConcatFieldPhase
from digital_land.phase.convert import ConvertPhase
from digital_land.phase.default import DefaultPhase
from digital_land.phase.factor import FactorPhase
from digital_land.phase.filter import FilterPhase
from digital_land.phase.harmonise import HarmonisePhase
from digital_land.phase.lookup import EntityLookupPhase, FactLookupPhase
from digital_land.phase.map import MapPhase
from digital_land.phase.migrate import MigratePhase
from digital_land.phase.normalise import NormalisePhase
from digital_land.phase.organisation import OrganisationPhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.patch import PatchPhase
from digital_land.phase.pivot import PivotPhase
from digital_land.phase.prefix import EntityPrefixPhase
from digital_land.phase.priority import PriorityPhase
from digital_land.phase.prune import FieldPrunePhase, EntityPrunePhase, FactPrunePhase
from digital_land.phase.reference import EntityReferencePhase, FactReferencePhase
from digital_land.phase.save import SavePhase


def chain_phases(phases):
    def add(f, g):
        return lambda x: g.process(f(x))

    return functools.reduce(add, phases, lambda phase: phase)


def run_pipeline(*args):
    """Backward compatible wrapper.

    Prefer calling `Pipeline.run(*phases)` on a configured Pipeline instance.
    """
    logging.debug(f"run_pipeline {args}")
    Pipeline.run_phases(*args)


# TODO should we remove loading from init? it makes it harder to test
# and what if you only wanted to load specific files
# TODO replace with config models which load is handled by them
class Pipeline:
    def __init__(self, path, dataset, specification=None, config=None):
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
        self.redirect_lookup = {}

        self.specification = specification
        self.config = config

        self.load_column()
        self.load_skip_patterns()
        self.load_patch()
        self.load_default_fields()
        self.load_default_values()
        self.load_concat()
        self.load_combine_fields()
        self.load_migrate()
        self.load_lookup()
        self.load_redirect_lookup()
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
            row["dataset"] = row.get("dataset", "") or row.get("pipeline", "")
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
            resource = row.get("resource", "")
            endpoint = row.get("endpoint", "")

            if resource:
                record = self.filter.setdefault(resource, {})
            elif endpoint:
                record = self.filter.setdefault(endpoint, {})
            else:
                record = self.filter.setdefault("", {})

            record[row["field"]] = row["pattern"]

    def load_skip_patterns(self):
        for row in self.reader("skip.csv"):
            resource = row.get("resource", "")
            endpoint = row.get("endpoint", "")

            if resource:
                record = self.skip_pattern.setdefault(resource, [])
            elif endpoint:
                record = self.skip_pattern.setdefault(endpoint, [])
            else:
                record = self.skip_pattern.setdefault("", [])

            record.append(row["pattern"])

    def load_patch(self):
        for row in self.reader("patch.csv"):
            resource = row.get("resource", "")
            endpoint = row.get("endpoint", "")

            if resource:
                record = self.patch.setdefault(resource, {})
            elif endpoint:
                record = self.patch.setdefault(endpoint, {})
            else:
                record = self.patch.setdefault("", {})

            row["field"] = row.get("field", "")
            row["pattern"] = row.get("pattern", "")

            record = record.setdefault(row["field"], {})
            record[row["pattern"]] = row["value"]

    def load_default_fields(self):
        # TBD: rename default-field.csv
        for row in self.reader("default.csv"):
            resource = row.get("resource", "")
            endpoint = row.get("endpoint", "")

            if resource:
                record = self.default_field.setdefault(resource, {})
            elif endpoint:
                record = self.default_field.setdefault(endpoint, {})
            else:
                record = self.default_field.setdefault("", {})

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
                "prepend": row.get("prepend", ""),
                "append": row.get("append", ""),
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
            # replace local-authority-eng while we migrate
            organisation = organisation.replace(
                "local-authority-eng", "local-authority"
            )
            resource_lookup[
                lookup_key(
                    prefix=prefix,
                    reference=reference,
                    organisation=organisation,
                )
            ] = row["entity"]

    def load_redirect_lookup(self):
        for row in self.file_reader("old-entity.csv"):
            old_entity = row.get("old-entity", "")
            entity = row.get("entity", "")
            status = row.get("status", "")
            if old_entity and status:
                self.redirect_lookup[old_entity] = {"entity": entity, "status": status}

    def filters(self, resource="", endpoints=[]):
        d = self.filter.get("", {}).copy()

        for endpoint in endpoints:
            endpoint_filters = self.filter.get(endpoint, {})
            d.update(endpoint_filters)

        if resource:
            resource_filters = self.filter.get(resource, {})
            d.update(resource_filters)

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
            if (
                general_columns[key] in endpoint_columns.values()
                or general_columns[key] in resource_columns.values()
            ):
                continue
            result[key] = general_columns[key]
        return result

    def skip_patterns(self, resource="", endpoints=[]):
        if not resource:
            return self.skip_pattern.get("", {})
        endpoint_patterns = []
        for endpoint in endpoints:
            endpoint_patterns.extend(self.skip_pattern.get(endpoint, []))

        return (
            self.skip_pattern.get(resource, [])
            + self.skip_pattern.get("", [])
            + endpoint_patterns
        )

    def patches(self, resource="", endpoints=[]):
        general_patch = self.patch.get("", {})
        if not resource:
            return general_patch

        resource_patch = self.patch.get(resource, {})
        endpoint_patch = {}

        for endpoint in endpoints:
            endpoint_patch = {**endpoint_patch, **self.patch.get(endpoint, {})}

        result = {**endpoint_patch, **resource_patch}

        # Merge any remaining general defaults into the result
        for field, patch in general_patch.items():
            if field not in result:
                result[field] = patch
            else:
                result[field] = {**patch, **result[field]}

        return result

    def default_fields(self, resource=None, endpoints=[]):
        config = self.default_field

        d = config.get("", {})

        for key, value in config.get(resource, {}).items():
            d[key] = value

        for endpoint in endpoints:
            for key, value in config.get(endpoint, {}).items():
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

    def redirect_lookups(self):
        return self.redirect_lookup

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

    @staticmethod
    def run_phases(*phases):
        """Execute a sequence of phases by composing their `.process()` pipelines.

        Phases are expected to be objects with a `process(iterable)` method.
        Historically the chain has been started with `None`.
        """
        chain = chain_phases([phase for phase in phases if phase])
        stream = chain(None)
        for _row in stream:
            pass

    def run(self, *phases):
        logging.debug(f"running {self.name} through {phases}")
        self.run_phases(*phases)

    def build_transform_phases(
        self,
        *,
        input_path,
        output_path,
        dataset_resource_log,
        converted_resource_log,
        issue_log,
        operational_issue_log,
        column_field_log,
        organisation,
        valid_category_values,
        endpoints=None,
        organisations=None,
        entry_date="",
        resource=None,
        null_path=None,
        converted_path=None,
        harmonised_output_path=None,
        save_harmonised=False,
    ):
        """Build the default resource->transformed phase list.

        This mirrors the legacy `commands.pipeline_run()` phase wiring, but keeps
        the execution responsibility inside Pipeline.
        """
        if self.specification is None:
            raise ValueError("Pipeline.specification is required to build phases")

        endpoints = endpoints or []
        organisations = organisations or []

        specification = self.specification
        dataset = self.name
        schema = specification.pipeline[dataset]["schema"]
        intermediate_fieldnames = specification.intermediate_fieldnames(self)

        # load pipeline configuration
        skip_patterns = self.skip_patterns(resource, endpoints)
        columns = self.columns(resource, endpoints=endpoints)
        concats = self.concatenations(resource, endpoints=endpoints)
        patches = self.patches(resource=resource, endpoints=endpoints)
        lookups = self.lookups(resource=resource)
        default_fields = self.default_fields(resource=resource, endpoints=endpoints)
        default_values = self.default_values(endpoints=endpoints)
        combine_fields = self.combine_fields(endpoints=endpoints)
        redirect_lookups = self.redirect_lookups()

        entity_range_min = specification.get_dataset_entity_min(dataset)
        entity_range_max = specification.get_dataset_entity_max(dataset)

        # resource specific default values
        if len(organisations) == 1:
            default_values["organisation"] = organisations[0]

        # need an entry-date for all entries and for facts
        if entry_date and "entry-date" not in default_values:
            default_values["entry-date"] = entry_date

        phases = [
            ConvertPhase(
                path=input_path,
                dataset_resource_log=dataset_resource_log,
                converted_resource_log=converted_resource_log,
                output_path=converted_path,
            ),
            NormalisePhase(skip_patterns=skip_patterns, null_path=null_path),
            ParsePhase(),
            ConcatFieldPhase(concats=concats, log=column_field_log),
            FilterPhase(filters=self.filters(resource)),
            MapPhase(
                fieldnames=intermediate_fieldnames,
                columns=columns,
                log=column_field_log,
            ),
            FilterPhase(filters=self.filters(resource, endpoints=endpoints)),
            PatchPhase(
                issues=issue_log,
                patches=patches,
            ),
            HarmonisePhase(
                field_datatype_map=specification.get_field_datatype_map(),
                issues=issue_log,
                dataset=dataset,
                valid_category_values=valid_category_values,
            ),
            DefaultPhase(
                default_fields=default_fields,
                default_values=default_values,
                issues=issue_log,
            ),
            MigratePhase(
                fields=specification.schema_field[schema],
                migrations=self.migrations(),
            ),
            OrganisationPhase(organisation=organisation, issues=issue_log),
            FieldPrunePhase(fields=specification.current_fieldnames(schema)),
            EntityReferencePhase(
                dataset=dataset,
                prefix=specification.dataset_prefix(dataset),
                issues=issue_log,
            ),
            EntityPrefixPhase(dataset=dataset),
            EntityLookupPhase(
                lookups=lookups,
                redirect_lookups=redirect_lookups,
                issue_log=issue_log,
                operational_issue_log=operational_issue_log,
                entity_range=[entity_range_min, entity_range_max],
            ),
            SavePhase(
                harmonised_output_path,
                fieldnames=intermediate_fieldnames,
                enabled=save_harmonised,
            ),
            EntityPrunePhase(dataset_resource_log=dataset_resource_log),
            PriorityPhase(config=self.config, providers=organisations),
            PivotPhase(),
            FactCombinePhase(issue_log=issue_log, fields=combine_fields),
            FactorPhase(),
            FactReferencePhase(
                field_typology_map=specification.get_field_typology_map(),
                field_prefix_map=specification.get_field_prefix_map(),
            ),
            FactLookupPhase(
                lookups=lookups,
                redirect_lookups=redirect_lookups,
                issue_log=issue_log,
                odp_collections=specification.get_odp_collections(),
            ),
            FactPrunePhase(),
            SavePhase(
                output_path,
                fieldnames=specification.factor_fieldnames(),
            ),
        ]

        return phases, combine_fields


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
        self.old_entity_path = Path(directory) / "old-entity.csv"
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
            raise RuntimeError(
                f"{len(extra_fields)} extra fields founds in lookup.csv ({','.join(list(extra_fields))})"
            )

        for row in reader:
            self.add_entry(row, is_new_entry=False)

    def get_max_entity(self, prefix, specification) -> int:
        if len(self.entries) == 0:
            return 0
        if not prefix:
            return 0

        dataset_prefix = specification.dataset_prefix(prefix)
        try:
            ret_val = max(
                [
                    int(entry["entity"])
                    for entry in self.entries
                    if (entry["prefix"] == prefix or entry["prefix"] == dataset_prefix)
                    and (entry.get("entity", None))
                ]
            )
            return ret_val
        except ValueError:
            return 0

    def save_csv(self, lookups_path=None, entries=None, old_entity_path=None):
        path = lookups_path or self.lookups_path

        entity_values = []
        if os.path.exists(path):
            reader = csv.DictReader(open(path, newline=""))
            for row in reader:
                entity_values.append(row["entity"])

        if entries is None:
            entries = self.entries

        os.makedirs(os.path.dirname(path), exist_ok=True)
        logging.debug("saving %s" % (path))
        f = open(path, "w", newline="")
        writer = csv.DictWriter(
            f, fieldnames=self.schema.fieldnames, extrasaction="ignore"
        )
        writer.writeheader()

        old_entity_file_path = old_entity_path or self.old_entity_path
        if os.path.exists(old_entity_file_path):
            old_entity_path = self.old_entity_path
            reader = csv.DictReader(open(old_entity_file_path, newline=""))

            for row in reader:
                entity_values.append(row["old-entity"])
                entity_values.append(row["entity"])

        new_entities = []
        get_entity = None
        minimum_generated_entity = None
        maximum_generated_entity = None
        for idx, entry in enumerate(entries):
            if not entry:
                continue
            else:
                if not entry.get("entity"):
                    while True:
                        generated_entity = self.entity_num_gen.next()

                        if generated_entity == get_entity:
                            print(
                                "There are no more entity numbers available within this dataset."
                            )
                            break

                        if get_entity is None:
                            get_entity = generated_entity

                        if str(generated_entity) not in entity_values:
                            entry["entity"] = generated_entity
                            if (
                                not minimum_generated_entity
                            ) or generated_entity < minimum_generated_entity:
                                minimum_generated_entity = generated_entity
                            if (
                                not maximum_generated_entity
                            ) or generated_entity > maximum_generated_entity:
                                maximum_generated_entity = generated_entity
                            new_entities.append(entry)
                            entity_values.append(str(generated_entity))
                            writer.writerow(entry)
                            break
                else:
                    writer.writerow(entry)
        if len(new_entities) > 0:
            print("Total number of new entities:", len(new_entities))
            print("Minimum generated entity number:", minimum_generated_entity)
            print("Maximum generated entity number:", maximum_generated_entity)
            print("\n")
        return new_entities

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
