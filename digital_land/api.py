import os
import sys
import json
import logging
from pathlib import Path

import canonicaljson

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
from .update import add_source_endpoint, get_failing_endpoints_from_registers
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
    def convert_cmd(self, input_path, output_path):
        if not output_path:
            output_path = self.default_output_path("converted", input_path)
        dataset_resource_log = DatasetResourceLog()
        run_pipeline(
            ConvertPhase(input_path, dataset_resource_log=dataset_resource_log),
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
            ConvertPhase(path=input_path, dataset_resource_log=dataset_resource_log),
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

    #
    #  configuration commands
    #
    @staticmethod
    def collection_check_endpoints_cmd(first_date, log_dir, endpoint_path, last_date):
        """find active endpoints that are failing during collection"""
        output = get_failing_endpoints_from_registers(
            log_dir, endpoint_path, first_date.date(), last_date.date()
        )
        print(canonicaljson.encode_canonical_json(output))

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