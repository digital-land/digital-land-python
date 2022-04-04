from collections import OrderedDict
import csv
import itertools
import os
import sys
import json
import logging
from pathlib import Path

from .collection import Collection, resource_path
from .collect import Collector
from .log import IssueLog, ColumnFieldLog, DatasetResourceLog
from .organisation import Organisation
from .package.dataset import DatasetPackage
from .phase.concat import ConcatFieldPhase
from .phase.convert import ConvertPhase
from .phase.dump import DumpPhase
from .phase.factor import FactorPhase
from .phase.filter import FilterPhase
from .phase.harmonise import HarmonisePhase
from .phase.lookup import EntityLookupPhase, FactLookupPhase
from .phase.map import MapPhase
from .phase.normalise import NormalisePhase
from .phase.parse import ParsePhase
from .phase.pivot import PivotPhase
from .phase.prefix import EntityPrefixPhase
from .phase.prune import EntityPrunePhase, FactPrunePhase
from .phase.reduce import ReducePhase
from .phase.reference import EntityReferencePhase, FactReferencePhase
from .phase.save import SavePhase
from .phase.migrate import MigratePhase
from .pipeline import Pipeline, run_pipeline
from .plugin import get_plugin_manager
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
        collection_dir,
        null_path,
        issue_dir,
        organisation_path,
        save_harmonised=False,
        column_field_dir=None,
        dataset_resource_dir=None,
        custom_temp_dir=None,
    ):
        resource = self.resource_from_path(input_path)
        dataset = schema = self.specification.pipeline[self.pipeline.name]["schema"]
        intermediate_fieldnames = self.specification.intermediate_fieldnames(
            self.pipeline
        )
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        organisation = Organisation(organisation_path, Path(self.pipeline.path))
        plugin_manager = get_plugin_manager()
        patches = self.pipeline.patches(resource)
        default_fieldnames = self.pipeline.default_fieldnames(resource)
        lookups = self.pipeline.lookups(resource)

        issue_log = IssueLog(dataset=dataset, resource=resource)
        column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)
        dataset_resource_log = DatasetResourceLog(dataset=dataset, resource=resource)

        run_pipeline(
            ConvertPhase(
                path=input_path,
                dataset_resource_log=dataset_resource_log,
                custom_temp_dir=custom_temp_dir,
            ),
            NormalisePhase(self.pipeline.skip_patterns(resource), null_path=null_path),
            ParsePhase(),
            MapPhase(
                fieldnames=intermediate_fieldnames,
                columns=self.pipeline.columns(resource),
                log=column_field_log,
            ),
            ConcatFieldPhase(
                concats=self.pipeline.concatenations(resource),
                log=column_field_log,
            ),
            FilterPhase(self.pipeline.filters(resource)),
            # TBD: break down this complicated phase
            HarmonisePhase(
                specification=self.specification,
                dataset=dataset,
                issues=issue_log,
                collection=collection,
                organisation_uri=organisation.organisation_uri,
                patches=patches,
                default_fieldnames=default_fieldnames,
                plugin_manager=plugin_manager,
            ),
            SavePhase(
                self.default_output_path("harmonised", input_path),
                fieldnames=intermediate_fieldnames,
                enabled=save_harmonised,
            ),
            MigratePhase(
                self.specification.schema_field[schema],
                self.pipeline.migrations(),
            ),
            ReducePhase(fields=self.specification.current_fieldnames(schema)),
            EntityReferencePhase(
                dataset=dataset,
                specification=self.specification,
            ),
            EntityPrefixPhase(dataset=dataset),
            EntityLookupPhase(lookups),
            EntityPrunePhase(
                issue_log=issue_log, dataset_resource_log=dataset_resource_log
            ),
            PivotPhase(),
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
        package.add_counts()

    def dataset_dump_cmd(self, input_path, output_path):
        cmd = (
            f"sqlite3 -header -csv {input_path} 'select * from entity;' > {output_path}"
        )
        logging.info(cmd)
        os.system(cmd)

    def dataset_dump_hoisted_cmd(self, csv_path, hoisted_csv_path):
        if not hoisted_csv_path:
            hoisted_csv_path = csv_path.replace(".csv", "-hoisted.csv")

        with open(csv_path, "r") as read_file, open(
            hoisted_csv_path, "w+"
        ) as write_file:
            reader = csv.DictReader(read_file)

            spec_field_names = [
                field.replace("-", "_")
                for field in itertools.chain(
                    *[
                        self.specification.current_fieldnames(schema)
                        for schema in self.specification.dataset_schema[self.dataset]
                    ]
                )
            ]
            reader_fieldnames = list(reader.fieldnames)
            reader_fieldnames.remove("json")
            hoisted_field_names = set(spec_field_names).difference(
                set(reader_fieldnames)
            )
            # Make sure we put hoisted fieldnames last
            field_names = reader_fieldnames + sorted(list(hoisted_field_names))

            writer = csv.DictWriter(write_file, fieldnames=field_names)
            writer.writeheader()
            for row in reader:
                row = OrderedDict(row)
                json_string = row.pop("json") or "{}"
                row.update(json.loads(json_string))
                snake_case_row = dict(
                    [(key.replace("-", "_"), val) for key, val in row.items()]
                )
                writer.writerow(snake_case_row)

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
