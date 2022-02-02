import os
import sys
import json
import logging
from pathlib import Path

import canonicaljson

from .collection import Collection, resource_path
from .collect import Collector
from .issues import Issues, IssuesFile
from .organisation import Organisation
from .package.dataset import DatasetPackage
from .phase.load import load_csv, load_csv_dict
from .phase.convert import ConvertPhase
from .phase.factor import FactorPhase
from .phase.filter import FilterPhase
from .phase.harmonise import HarmonisePhase
from .phase.lookup import LookupPhase
from .phase.map import MapPhase
from .phase.normalise import NormalisePhase
from .phase.parse import ParsePhase
from .phase.pivot import PivotPhase
from .phase.reduce import ReducePhase
from .phase.save import save, SavePhase
from .phase.transform import TransformPhase
from .pipeline import Pipeline
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
                "pipeline_name": self.pipeline_name,
                "pipeline_dir": self.pipeline_dir,
                "specification_dir": self.specification_dir,
            }
        )

    def __init__(self, debug, pipeline_name, pipeline_dir, specification_dir):
        # Save init vars for easy serialization/deserialization
        self.debug = debug
        self.dataset = self.pipeline_name = pipeline_name
        self.pipeline_dir = pipeline_dir
        self.specification_dir = specification_dir

        level = logging.DEBUG if debug else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
        self.pipeline = Pipeline(pipeline_dir, pipeline_name)
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
    def pipeline_collection_list_resources_cmd(self, collection_dir):
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        for resource in sorted(collection.resource.records):
            print(resource_path(resource, directory=collection_dir))

    def pipeline_collection_pipeline_makerules_cmd(self, collection_dir):
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        collection.pipeline_makerules()

    def pipeline_collection_save_csv_cmd(self, collection_dir):
        try:
            os.remove(Path(collection_dir) / "log.csv")
            os.remove(Path(collection_dir) / "resource.csv")
        except OSError:
            pass
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        collection.save_csv()

    #
    #  pipeline commands
    #
    def convert_cmd(self, input_path, output_path):
        if not output_path:
            output_path = self.default_output_path_for("converted", input_path)
        convert_phase = ConvertPhase()
        reader = convert_phase.process(input_path)
        if not reader:
            logging.error(f"Unable to convert {input_path}")
            sys.exit(2)
        save(reader, output_path)

    def normalise_cmd(self, input_path, output_path, null_path, skip_path):
        if not output_path:
            output_path = self.default_output_path_for("normalised", input_path)
        resource_hash = self.resource_hash_from(input_path)
        stream = load_csv(input_path)
        normalise_phase = NormalisePhase(
            self.pipeline.skip_patterns(resource_hash), null_path=null_path
        )
        stream = normalise_phase.process(stream)
        save(stream, output_path)

    def map_cmd(self, input_path, output_path):
        if not output_path:
            output_path = self.default_output_path_for("mapped", input_path)
        resource_hash = self.resource_hash_from(input_path)
        fieldnames = self.specification.intermediate_fieldnames(self.pipeline)
        map_phase = MapPhase(
            fieldnames,
            self.pipeline.columns(resource_hash),
            self.pipeline.concatenations(resource_hash),
        )
        stream = load_csv_dict(input_path)
        stream = map_phase.map(stream)
        save(stream, output_path, fieldnames=fieldnames)

    def filter_cmd(self, input_path, output_path):
        if not output_path:
            output_path = self.default_output_path_for("filtered", input_path)
        resource_hash = self.resource_hash_from(input_path)
        fieldnames = self.specification.intermediate_fieldnames(self.pipeline)
        filter_phase = FilterPhase(self.pipeline.filters(resource_hash))
        stream = load_csv_dict(input_path)
        stream = filter_phase.process(stream)
        save(stream, output_path, fieldnames=fieldnames)

    def harmonise_cmd(self, input_path, output_path, issue_dir, organisation_path):
        if not output_path:
            output_path = self.default_output_path_for("harmonised", input_path)
        resource_hash = self.resource_hash_from(input_path)
        issues = Issues()
        collection = Collection()
        collection.load()
        organisation_uri = Organisation(
            organisation_path, Path(self.pipeline.path)
        ).organisation_uri
        patch = self.pipeline.patches(resource_hash)
        fieldnames = self.specification.intermediate_fieldnames(self.pipeline)
        pm = get_plugin_manager()
        harmonise_phase = HarmonisePhase(
            self.specification,
            self.pipeline,
            issues,
            collection,
            organisation_uri,
            patch,
            pm,
        )
        stream = load_csv_dict(input_path)
        stream = harmonise_phase.process(stream)
        save(stream, output_path, fieldnames=fieldnames)
        issues_file = IssuesFile(path=os.path.join(issue_dir, resource_hash + ".csv"))
        issues_file.write_issues(issues)

    def transform_cmd(self, input_path, output_path, organisation_path):
        if not output_path:
            output_path = self.default_output_path_for("transformed", input_path)
        organisation = Organisation(organisation_path, Path(self.pipeline.path))
        schema = self.specification.pipeline[self.pipeline.name]["schema"]
        transform_phase = TransformPhase(
            schema,
            self.pipeline.transformations(),
            organisation.organisation,
        )
        stream = load_csv_dict(input_path)
        stream = transform_phase.process(stream)
        save(stream, output_path, self.specification.current_fieldnames(schema))

    def pipeline_cmd(
        self,
        input_path,
        output_path,
        collection_dir,
        null_path,
        issue_dir,
        organisation_path,
        save_harmonised,
    ):
        resource_hash = self.resource_hash_from(input_path)
        schema = self.specification.pipeline[self.pipeline.name]["schema"]
        intermediate_fieldnames = self.specification.intermediate_fieldnames(
            self.pipeline
        )
        key_field = self.specification.key_field(schema)
        patch = self.pipeline.patches(resource_hash)
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        organisation = Organisation(organisation_path, Path(self.pipeline.path))
        pm = get_plugin_manager()
        issues = Issues()

        self.pipeline.run(
            input_path,
            phases=[
                ConvertPhase(),
                NormalisePhase(
                    self.pipeline.skip_patterns(resource_hash), null_path=null_path
                ),
                ParsePhase(),
                MapPhase(
                    intermediate_fieldnames,
                    self.pipeline.columns(resource_hash),
                    self.pipeline.concatenations(resource_hash),
                ),
                FilterPhase(self.pipeline.filters(resource_hash)),
                HarmonisePhase(
                    self.specification,
                    self.pipeline,
                    issues,
                    collection,
                    organisation.organisation_uri,
                    patch,
                    pm,
                ),
                SavePhase(
                    self.default_output_path_for("harmonised", input_path),
                    intermediate_fieldnames,
                    enabled=save_harmonised,
                ),
                TransformPhase(
                    self.specification.schema_field[schema],
                    self.pipeline.transformations(),
                    organisation.organisation,
                ),
                ReducePhase(self.specification.current_fieldnames(schema)),
                LookupPhase(
                    lookups=self.pipeline.lookups(resource_hash),
                    key_field=key_field,
                ),
                PivotPhase(),
                FactorPhase(),
                SavePhase(
                    output_path,
                    fieldnames=self.specification.factor_fieldnames(),
                ),
            ],
        )

        # TBD: move issues to the stream, make saving them a Phase
        issues_file = IssuesFile(path=os.path.join(issue_dir, resource_hash + ".csv"))
        issues_file.write_issues(issues)

    #
    #  build dataset from processed resources
    #
    def dataset_create_cmd(self, input_paths, output_path):
        if not output_path:
            print("missing output path")
            sys.exit(2)
        package = DatasetPackage(self.dataset)
        package.create()
        for path in input_paths:
            package.load_transformed(path)
        package.load_entities()

    def dataset_dump_cmd(self, input_path, output_path):
        cmd = f"sqlite3 -header -csv {input_path} 'select * from entity;' > {output_path}"
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
    def resource_hash_from(path):
        return Path(path).stem

    def default_output_path_for(self, command, input_path):
        directory = "" if command in ["harmonised", "transformed"] else "var/"
        return f"{directory}{command}/{self.resource_hash_from(input_path)}.csv"
