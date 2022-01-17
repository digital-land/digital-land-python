import functools
import itertools
import logging
import os
import sys
from pathlib import Path

import canonicaljson

from .collect import Collector
from .collection import Collection, resource_path
from .convert import Converter
from .entry_loader import EntryLoader
from .filter import Filterer
from .harmonise import Harmoniser
from .index import Indexer
from .issues import Issues, IssuesFile
from .load import LineConverter, load_csv, load_csv_dict
from .map import Mapper
from .model.entity import Entity
from .normalise import Normaliser
from .organisation import Organisation
from .pipeline import Pipeline
from .plugin import get_plugin_manager
from .repository.entry_repository import EntryRepository
from .save import save
from .schema import Schema
from .lookup import Lookup
from .slug import Slugger
from .specification import Specification
from .transform import Transformer
from .update import add_source_endpoint, get_failing_endpoints_from_registers
from .datasette.docker import build_container


class DigitalLandApi(object):
    pipeline: Pipeline
    specification: Specification

    def __init__(self, debug, pipeline_name, pipeline_dir, specification_dir):
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
    @staticmethod
    def index_cmd():
        # TBD: replace with Collection()
        indexer = Indexer()
        indexer.index()

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
        converter = Converter(self.pipeline.conversions())
        reader = converter.convert(input_path)
        if not reader:
            logging.error(f"Unable to convert {input_path}")
            sys.exit(2)
        save(reader, output_path)

    def normalise_cmd(self, input_path, output_path, null_path, skip_path):
        if not output_path:
            output_path = self.default_output_path_for("normalised", input_path)
        resource_hash = self.resource_hash_from(input_path)
        stream = load_csv(input_path)
        normaliser = Normaliser(
            self.pipeline.skip_patterns(resource_hash), null_path=null_path
        )
        stream = normaliser.normalise(stream)
        save(stream, output_path)

    def map_cmd(self, input_path, output_path):
        if not output_path:
            output_path = self.default_output_path_for("mapped", input_path)
        resource_hash = self.resource_hash_from(input_path)
        fieldnames = self.intermediary_fieldnames(self.specification, self.pipeline)
        mapper = Mapper(
            fieldnames,
            self.pipeline.columns(resource_hash),
            self.pipeline.concatenations(resource_hash),
        )
        stream = load_csv_dict(input_path)
        stream = mapper.map(stream)
        save(stream, output_path, fieldnames=fieldnames)

    def filter_cmd(self, input_path, output_path):
        if not output_path:
            output_path = self.default_output_path_for("filtered", input_path)
        resource_hash = self.resource_hash_from(input_path)
        fieldnames = self.intermediary_fieldnames(self.specification, self.pipeline)
        filterer = Filterer(self.pipeline.filters(resource_hash))
        stream = load_csv_dict(input_path)
        stream = filterer.filter(stream)
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
        fieldnames = self.intermediary_fieldnames(self.specification, self.pipeline)
        pm = get_plugin_manager()
        harmoniser = Harmoniser(
            self.specification,
            self.pipeline,
            issues,
            collection,
            organisation_uri,
            patch,
            pm,
        )
        stream = load_csv_dict(input_path)
        stream = harmoniser.harmonise(stream)
        save(stream, output_path, fieldnames=fieldnames)
        issues_file = IssuesFile(path=os.path.join(issue_dir, resource_hash + ".csv"))
        issues_file.write_issues(issues)

    def transform_cmd(self, input_path, output_path, organisation_path):
        if not output_path:
            output_path = self.default_output_path_for("transformed", input_path)
        organisation = Organisation(organisation_path, Path(self.pipeline.path))
        schema = self.specification.pipeline[self.pipeline.name]["schema"]
        transformer = Transformer(
            schema,
            self.pipeline.transformations(),
            organisation.organisation,
        )
        stream = load_csv_dict(input_path)
        stream = transformer.transform(stream)
        save(stream, output_path, self.specification.current_fieldnames(schema))

    @staticmethod
    def load_entries_cmd(input_paths, output_path):
        if not output_path:
            print("missing output path")
            sys.exit(2)
        repo = EntryRepository(output_path, create=True)
        loader = EntryLoader(repo)
        total = len(input_paths)
        for idx, path in enumerate(input_paths, start=1):
            logging.info("loading %s [%s/%s]", path, idx, total)
            stream = load_csv_dict(path, include_line_num=True)
            loader.load(stream)

    def build_dataset_cmd(self, input_path, output_path):
        repo = EntryRepository(input_path)
        slugs = repo.list_slugs()
        logging.info("building dataset with %s slugs", len(slugs))
        schema = self.specification.pipeline[self.pipeline.name]["schema"]
        output = filter(
            lambda x: x["row"],
            (
                {
                    "row": Entity(
                        repo.find_by_slug(slug),
                        schema,
                    ).snapshot()
                }
                for slug in slugs
            ),
        )
        save(
            output,
            output_path,
            self.specification.current_fieldnames(schema),
        )

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
        organisation = Organisation(organisation_path, Path(self.pipeline.path))
        issues = Issues()
        schema = self.specification.pipeline[self.pipeline.name]["schema"]
        fieldnames = self.intermediary_fieldnames(self.specification, self.pipeline)
        patch = self.pipeline.patches(resource_hash)
        collection = Collection(name=None, directory=collection_dir)
        collection.load()
        line_converter = LineConverter()
        pm = get_plugin_manager()
        converter = Converter(self.pipeline.conversions())
        normaliser = Normaliser(
            self.pipeline.skip_patterns(resource_hash), null_path=null_path
        )
        mapper = Mapper(
            fieldnames,
            self.pipeline.columns(resource_hash),
            self.pipeline.concatenations(resource_hash),
        )
        filterer = Filterer(self.pipeline.filters(resource_hash))
        harmoniser = Harmoniser(
            self.specification,
            self.pipeline,
            issues,
            collection,
            organisation.organisation_uri,
            patch,
            pm,
        )
        transformer = Transformer(
            self.specification.schema_field[schema],
            self.pipeline.transformations(),
            organisation.organisation,
        )
        key_field = self.specification.key_field(schema)
        lookup = Lookup(
            lookups=self.pipeline.lookups(resource_hash),
            key_field=key_field,
        )
        slugger = Slugger(
            self.specification.pipeline[self.pipeline.name].get("slug-prefix", None),
            key_field,
            self.specification.pipeline[self.pipeline.name].get("scope-field", None),
        )
        pipeline_funcs = [
            converter.convert,
            normaliser.normalise,
            line_converter.convert,
            mapper.map,
            filterer.filter,
            harmoniser.harmonise,
        ]
        if save_harmonised:
            harmonised_path = output_path.replace("transformed", "harmonised")
            if harmonised_path == output_path:
                raise ValueError("cannot write harmonised file due to name clash")

            def saver(reader):
                output_tap, save_tap = itertools.tee(reader)
                save(
                    save_tap,
                    harmonised_path,
                    fieldnames=self.intermediary_fieldnames(
                        self.specification, self.pipeline
                    ),
                )
                yield from output_tap

            pipeline_funcs.append(saver)
        pipeline_funcs = pipeline_funcs + [
            transformer.transform,
            lookup.lookup,
            slugger.slug,
        ]
        pipeline = self.compose(*pipeline_funcs)
        output = pipeline(input_path)
        save(
            output,
            output_path,
            fieldnames=self.specification.current_fieldnames(schema),
        )
        issues_file = IssuesFile(path=os.path.join(issue_dir, resource_hash + ".csv"))
        issues_file.write_issues(issues)

    # Endpoint commands
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

    @staticmethod
    def intermediary_fieldnames(specification, pipeline):
        schema = specification.pipeline[pipeline.name]["schema"]
        fieldnames = specification.schema_field[schema].copy()
        replacement_fields = list(pipeline.transformations().keys())
        for field in replacement_fields:
            if field in fieldnames:
                fieldnames.remove(field)
        return fieldnames

    def default_output_path_for(self, command, input_path):
        return f"var/{command}/{self.resource_hash_from(input_path)}.csv"

    @staticmethod
    def compose(*functions):
        def compose2(f, g):
            return lambda x: g(f(x))

        return functools.reduce(compose2, functions, lambda x: x)
