from collections import OrderedDict
import csv
import itertools
import os
import sys
import json
import logging
from pathlib import Path

import geojson
import shapely

from .collection import Collection, resource_path
from .collect import Collector
from .log import IssueLog, ColumnFieldLog, DatasetResourceLog
from .organisation import Organisation
from .package.dataset import DatasetPackage
from .phase.concat import ConcatFieldPhase
from .phase.convert import ConvertPhase
from .phase.default import DefaultPhase
from .phase.dump import DumpPhase
from .phase.factor import FactorPhase
from .phase.filter import FilterPhase
from .phase.harmonise import HarmonisePhase
from .phase.lookup import EntityLookupPhase, FactLookupPhase
from .phase.map import MapPhase
from .phase.normalise import NormalisePhase
from .phase.organisation import OrganisationPhase
from .phase.patch import PatchPhase
from .phase.parse import ParsePhase
from .phase.pivot import PivotPhase
from .phase.prefix import EntityPrefixPhase
from .phase.prune import FieldPrunePhase, EntityPrunePhase, FactPrunePhase
from .phase.combine import FactCombinePhase
from .phase.reference import EntityReferencePhase, FactReferencePhase
from .phase.save import SavePhase
from .phase.migrate import MigratePhase
from .pipeline import Pipeline, run_pipeline
from .schema import Schema
from .specification import Specification
from .update import add_source_endpoint
from .datasette.docker import build_container


class DigitalLandApi(object):
    pipeline: Pipeline
    specification: Specification

    def to_json(self):
        """
        Returns a JSON-encoded string containing the constructor arguments
        """
        return json.dumps(
            {
                "debug": self.debug,
                "dataset": self.dataset,
                "pipeline_dir": self.pipeline_dir,
                "specification_dir": self.specification_dir,
            }
        )

    def __init__(self, debug, dataset, pipeline_dir, specification_dir):
        # Save init vars for easy serialization/deserialization
        self.debug = debug
        self.dataset = dataset
        self.pipeline_dir = pipeline_dir
        self.specification_dir = specification_dir
        self.collection = None

        level = logging.DEBUG if debug else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
        self.pipeline = Pipeline(pipeline_dir, dataset)
        self.specification = Specification(specification_dir)

    def fetch_cmd(self, url):
        """fetch a single source endpoint URL, and add it to the collection"""
        collector = Collector(self.pipeline.name)
        collector.fetch(url)

    def collect_cmd(self, endpoint_path, collection_dir):
        """fetch the sources listed in the endpoint-url column of the ENDPOINT_PATH CSV file"""
        collector = Collector(self.pipeline.name, Path(collection_dir))
        collector.collect(endpoint_path)

    #
    #  collection commands
    #  TBD: make sub commands
    #
    def collection_list_resources_cmd(self, collection_dir):
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        for resource in sorted(collection.resource.records):
            print(resource_path(resource, directory=collection_dir))

    def collection_pipeline_makerules_cmd(self, collection_dir):
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        collection.pipeline_makerules()

    def collection_save_csv_cmd(self, collection_dir):
        try:
            os.remove(Path(collection_dir) / "log.csv")
            os.remove(Path(collection_dir) / "resource.csv")
        except OSError:
            pass
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        collection.save_csv()

    #
    #  Airflow spike entry point
    def pipeline_resource_mapping_for_collection(self, collection_dir):
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        collection.load_log_items(directory=collection_dir)
        return collection.dataset_resource_map()

    #
    #  pipeline commands
    #
    def convert_cmd(self, input_path, output_path, custom_temp_dir=None):
        if not output_path:
            output_path = self.default_output_path("converted", input_path)
        dataset_resource_log = DatasetResourceLog()
        run_pipeline(
            ConvertPhase(
                input_path,
                dataset_resource_log=dataset_resource_log,
                custom_temp_dir=custom_temp_dir,
            ),
            DumpPhase(output_path),
        )
        dataset_resource_log.save(f=sys.stdout)

    def pipeline_cmd(
        self,
        input_path,
        output_path,
        collection_dir="./collection",  # TBD: remove, replaced by endpoints, organisations and entry_date
        null_path=None,  # TBD: remove this
        issue_dir=None,
        organisation_path=None,
        save_harmonised=False,
        column_field_dir=None,
        dataset_resource_dir=None,
        custom_temp_dir=None,  # TBD: rename to "tmpdir"
        endpoints=[],
        organisations=[],
        entry_date="",
    ):
        resource = self.resource_from_path(input_path)
        dataset = self.dataset
        schema = self.specification.pipeline[self.pipeline.name]["schema"]
        intermediate_fieldnames = self.specification.intermediate_fieldnames(
            self.pipeline
        )
        issue_log = IssueLog(dataset=dataset, resource=resource)
        column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)
        dataset_resource_log = DatasetResourceLog(dataset=dataset, resource=resource)

        # load pipeline configuration
        skip_patterns = self.pipeline.skip_patterns(resource)
        columns = self.pipeline.columns(resource)
        concats = self.pipeline.concatenations(resource)
        patches = self.pipeline.patches(resource=resource)
        lookups = self.pipeline.lookups(resource=resource)
        default_fields = self.pipeline.default_fields(resource=resource)
        default_values = self.pipeline.default_values(endpoints=endpoints)
        combine_fields = self.pipeline.combine_fields(endpoints=endpoints)

        # load organisations
        organisation = Organisation(organisation_path, Path(self.pipeline.path))

        # load the resource default values from the collection
        if not endpoints:
            collection = Collection(name=None, directory=collection_dir)
            collection.load()
            endpoints = collection.resource_endpoints(resource)
            organisations = collection.resource_organisations(resource)
            entry_date = collection.resource_start_date(resource)

        # resource specific default values
        if len(organisations) == 1:
            default_values["organisation"] = organisations[0]

        if entry_date:
            default_values["entry-date"] = entry_date

        run_pipeline(
            ConvertPhase(
                path=input_path,
                dataset_resource_log=dataset_resource_log,
                custom_temp_dir=custom_temp_dir,
            ),
            NormalisePhase(skip_patterns=skip_patterns, null_path=null_path),
            ParsePhase(),
            ConcatFieldPhase(concats=concats, log=column_field_log),
            MapPhase(
                fieldnames=intermediate_fieldnames,
                columns=columns,
                log=column_field_log,
            ),
            FilterPhase(filters=self.pipeline.filters(resource)),
            PatchPhase(
                issues=issue_log,
                patches=patches,
            ),
            HarmonisePhase(
                specification=self.specification,
                issues=issue_log,
            ),
            DefaultPhase(
                default_fields=default_fields,
                default_values=default_values,
                issues=issue_log,
            ),
            # TBD: move migrating columns to fields to be immediately after map
            # this will simplify harmonisation and remove intermediate_fieldnames
            # but effects brownfield-land and other pipelines which operate on columns
            MigratePhase(
                fields=self.specification.schema_field[schema],
                migrations=self.pipeline.migrations(),
            ),
            OrganisationPhase(organisation=organisation),
            FieldPrunePhase(fields=self.specification.current_fieldnames(schema)),
            EntityReferencePhase(
                dataset=dataset,
                specification=self.specification,
            ),
            EntityPrefixPhase(dataset=dataset),
            EntityLookupPhase(lookups),
            SavePhase(
                self.default_output_path("harmonised", input_path),
                fieldnames=intermediate_fieldnames,
                enabled=save_harmonised,
            ),
            EntityPrunePhase(
                issue_log=issue_log, dataset_resource_log=dataset_resource_log
            ),
            PivotPhase(),
            FactCombinePhase(issue_log=issue_log, fields=combine_fields),
            FactorPhase(),
            FactReferencePhase(dataset=dataset, specification=self.specification),
            FactLookupPhase(lookups),
            FactPrunePhase(),
            SavePhase(
                output_path,
                fieldnames=self.specification.factor_fieldnames(),
            ),
        )

        issue_log.save(os.path.join(issue_dir, resource + ".csv"))
        column_field_log.save(os.path.join(column_field_dir, resource + ".csv"))
        dataset_resource_log.save(os.path.join(dataset_resource_dir, resource + ".csv"))

    #
    #  build dataset from processed resources
    #
    def dataset_create_cmd(self, input_paths, output_path, organisation_path):
        if not output_path:
            print("missing output path")
            sys.exit(2)
        organisation = Organisation(organisation_path, Path(self.pipeline.path))
        package = DatasetPackage(
            self.dataset,
            organisation=organisation,
            path=output_path,
            specification_dir=self.specification_dir,
        )
        package.create()
        for path in input_paths:
            package.load_transformed(path)
        package.load_entities()

        old_entity_path = os.path.join(self.pipeline.path, "old-entity.csv")
        if os.path.exists(old_entity_path):
            package.load_old_entities(old_entity_path)

        package.add_counts()

    def dataset_dump_cmd(self, input_path, output_path):
        cmd = (
            f"sqlite3 -header -csv {input_path} 'select * from entity;' > {output_path}"
        )
        logging.info(cmd)
        os.system(cmd)

    def dataset_dump_flattened_cmd(self, csv_path, flattened_dir):

        if isinstance(csv_path, str):
            path = Path(csv_path)
            dataset_name = path.stem
        elif isinstance(csv_path, Path):
            dataset_name = csv_path.stem
        else:
            logging.error(f"Can't extract datapackage name from {csv_path}")
            sys.exit(-1)

        flattened_csv_path = os.path.join(flattened_dir, f"{dataset_name}.csv")
        with open(csv_path, "r") as read_file, open(
            flattened_csv_path, "w+"
        ) as write_file:
            reader = csv.DictReader(read_file)

            spec_field_names = [
                field
                for field in itertools.chain(
                    *[
                        self.specification.current_fieldnames(schema)
                        for schema in self.specification.dataset_schema[self.dataset]
                    ]
                )
            ]
            reader_fieldnames = [
                field.replace("_", "-")
                for field in list(reader.fieldnames)
                if field != "json"
            ]

            flattened_field_names = set(spec_field_names).difference(
                set(reader_fieldnames)
            )
            # Make sure we put flattened fieldnames last
            field_names = reader_fieldnames + sorted(list(flattened_field_names))

            writer = csv.DictWriter(write_file, fieldnames=field_names)
            writer.writeheader()
            entities = []
            for row in reader:
                row.pop("geojson", None)
                row = OrderedDict(row)
                json_string = row.pop("json") or "{}"
                row.update(json.loads(json_string))
                kebab_case_row = dict(
                    [(key.replace("_", "-"), val) for key, val in row.items()]
                )
                writer.writerow(kebab_case_row)
                entities.append(kebab_case_row)

        # write the entities to json file as well
        flattened_json_path = os.path.join(flattened_dir, f"{dataset_name}.json")
        with open(flattened_json_path, "w") as out_json:
            out_json.write(json.dumps({"entities": entities}))

        features = []
        if len(entities) > 0 and entities[0]["typology"] == "geography":
            for entity in entities:
                wkt = entity.pop("geometry", None)
                if wkt is not None:
                    try:
                        geometry = shapely.wkt.loads(wkt)
                        feature = geojson.Feature(geometry=geometry)
                        feature["properties"] = entity
                        features.append(feature)
                    except Exception as e:
                        logging.error(f"Error loading wkt from {entity['entity']}")
                        logging.error(e)
                else:
                    wkt = entity.pop("point", None)
                    if wkt is not None:
                        try:
                            geometry = shapely.wkt.loads(wkt)
                            feature = geojson.Point(geometry=geometry)
                            feature["properties"] = entity
                            features.append(feature)
                        except Exception as e:
                            logging.error(f"Error loading wkt from {entity['entity']}")
                            logging.error(e)

        if features:
            feature_collection = geojson.FeatureCollection(features=features)
            geojson_path = os.path.join(flattened_dir, f"{dataset_name}.geojson")
            with open(geojson_path, "w") as out_geojson:
                out_geojson.write(geojson.dumps(feature_collection))

    def expectation_cmd(self, results_path, sqlite_dataset_path, data_quality_yaml):
        from .expectations.main import run_dq_suite

        run_dq_suite(results_path, sqlite_dataset_path, data_quality_yaml)

    #
    #  configuration commands
    #
    @staticmethod
    def collection_add_source_cmd(entry, collection, endpoint_url, collection_dir):
        """
        followed by a sequence of optional name and value pairs including the following names:
        "attribution", "licence", "pipelines", "status", "plugin",
        "parameters", "start-date", "end-date"
        """
        entry["collection"] = collection
        entry["endpoint-url"] = endpoint_url
        allowed_names = set(
            list(Schema("endpoint").fieldnames) + list(Schema("source").fieldnames)
        )
        for key in entry.keys():
            if key not in allowed_names:
                logging.error(f"unrecognised argument '{key}'")
                sys.exit(2)
        add_source_endpoint(entry, directory=collection_dir)

    #
    #  datasette commands
    #
    @staticmethod
    def build_datasette(tag, data_dir, ext, options):
        datasets = [f"{d}" for d in Path(data_dir).rglob(f"*.{ext}")]
        for dataset in datasets:
            if not Path(dataset).exists():
                print(f"{dataset} not found")
                sys.exit(1)
        container_id, name = build_container(datasets, tag, options)
        print("%s dataset successfully packaged" % len(datasets))
        print(f"container_id: {container_id}")
        if name:
            print(f"name: {name}")

    @staticmethod
    def resource_from_path(path):
        return Path(path).stem

    def default_output_path(self, command, input_path):
        directory = "" if command in ["harmonised", "transformed"] else "var/"
        return f"{directory}{command}/{self.resource_from_path(input_path)}.csv"
