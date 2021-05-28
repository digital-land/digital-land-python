import functools
import itertools
import logging
import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import canonicaljson
import click

from .collect import Collector
from .collection import Collection, resource_path
from .convert import Converter
from .entry_loader import EntryLoader
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
from .slug import Slugger
from .specification import Specification
from .transform import Transformer
from .update import add_source_endpoint, get_failing_endpoints_from_registers

PIPELINE = None
SPECIFICATION = None


# Custom decorators for common command arguments
def input_output_path(f):
    arguments = [
        click.argument("input-path", type=click.Path(exists=True)),
        click.argument("output-path", type=click.Path(), default=""),
    ]
    return functools.reduce(lambda x, arg: arg(x), reversed(arguments), f)


def pipeline_name(f):
    return click.option("--pipeline-name", "-n", type=click.STRING)(f)


def pipeline_dir(f):
    return click.option("--pipeline-dir", "-p", type=click.Path(), default="pipeline/")(
        f
    )


def collection_dir(f):
    return click.option(
        "--collection-dir",
        "-c",
        type=click.Path(exists=True),
        default="collection/",
    )(f)


def specification_dir(f):
    return click.option(
        "--specification-dir",
        "-s",
        type=click.Path(exists=True),
        default="specification/",
    )(f)


def issue_dir(f):
    return click.option(
        "--issue-dir", "-i", type=click.Path(exists=True), default="issue/"
    )(f)


def endpoint_path(f):
    return click.option(
        "--endpoint-path",
        type=click.Path(exists=True),
        default="collection/endpoint.csv",
    )(f)


def source_path(f):
    return click.option(
        "--source-path",
        type=click.Path(exists=True),
        default="collection/source.csv",
    )(f)


def organisation_path(f):
    return click.option(
        "--organisation-path",
        "-o",
        type=click.Path(exists=True),
        default="var/cache/organisation.csv",
    )(f)


@click.group()
@click.option("-d", "--debug/--no-debug", default=False)
@pipeline_name
@pipeline_dir
@specification_dir
def cli(debug, pipeline_name, pipeline_dir, specification_dir):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    global PIPELINE
    global SPECIFICATION
    PIPELINE = Pipeline(pipeline_dir, pipeline_name)
    SPECIFICATION = Specification(specification_dir)


@cli.command("fetch")
@click.argument("url")
def fetch_cmd(url):
    """fetch a single source endpoint URL, and add it to the collection"""
    collector = Collector(PIPELINE.name)
    collector.fetch(url)


@cli.command("collect")
@click.argument(
    "endpoint-path",
    type=click.Path(exists=True),
    default="collection/endpoint.csv",
)
@collection_dir
def collect_cmd(endpoint_path, collection_dir):
    """fetch the sources listed in the endpoint-url column of the ENDPOINT_PATH CSV file"""
    collector = Collector(PIPELINE.name, Path(collection_dir))
    collector.collect(endpoint_path)


#
#  collection commands
#  TBD: make sub commands
#
@cli.command(
    "index",
    short_help="create collection indices",
)
def index_cmd():
    # TBD: replace with Collection()
    indexer = Indexer()
    indexer.index()


@cli.command("collection-list-resources", short_help="list resources for a pipeline")
@collection_dir
def pipeline_collection_list_resources_cmd(collection_dir):
    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    for resource in sorted(collection.resource.records):
        print(resource_path(resource, directory=collection_dir))


@cli.command(
    "collection-pipeline-makerules",
    short_help="generate pipeline makerules for a collection",
)
@collection_dir
def pipeline_collection_pipeline_makerules_cmd(collection_dir):
    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    collection.pipeline_makerules()


@cli.command("collection-save-csv", short_help="save collection as CSV package")
@collection_dir
def pipeline_collection_save_csv_cmd(collection_dir):
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
@cli.command("convert", short_help="convert to a well-formed, UTF-8 encoded CSV file")
@input_output_path
def convert_cmd(input_path, output_path):
    if not output_path:
        output_path = default_output_path_for("converted", input_path)

    converter = Converter(PIPELINE.conversions())
    reader = converter.convert(input_path)
    if not reader:
        logging.error(f"Unable to convert {input_path}")
        sys.exit(2)
    save(reader, output_path)


@cli.command("normalise", short_help="removed padding, drop empty rows")
@input_output_path
@click.option(
    "--null-path",
    type=click.Path(exists=True),
    help="patterns for null fields",
    default=None,
)
@click.option(
    "--skip-path",
    type=click.Path(exists=True),
    help="patterns for skipped lines",
    default=None,
)
def normalise_cmd(input_path, output_path, null_path, skip_path):
    if not output_path:
        output_path = default_output_path_for("normalised", input_path)

    resource_hash = resource_hash_from(input_path)
    stream = load_csv(input_path)
    normaliser = Normaliser(PIPELINE.skip_patterns(resource_hash), null_path=null_path)
    stream = normaliser.normalise(stream)
    save(stream, output_path)


@cli.command("map", short_help="map misspelt column names to those in a pipeline")
@input_output_path
def map_cmd(input_path, output_path):
    if not output_path:
        output_path = default_output_path_for("mapped", input_path)

    resource_hash = resource_hash_from(input_path)
    fieldnames = intermediary_fieldnames(SPECIFICATION, PIPELINE)
    mapper = Mapper(
        fieldnames,
        PIPELINE.columns(resource_hash),
        PIPELINE.concatenations(resource_hash),
    )
    stream = load_csv_dict(input_path)
    stream = mapper.map(stream)
    save(stream, output_path, fieldnames=fieldnames)


@cli.command(
    "harmonise",
    short_help="strip whitespace and null fields, remove blank rows and columns",
)
@input_output_path
@issue_dir
@organisation_path
def harmonise_cmd(input_path, output_path, issue_dir, organisation_path):
    if not output_path:
        output_path = default_output_path_for("harmonised", input_path)

    resource_hash = resource_hash_from(input_path)
    issues = Issues()

    collection = Collection()
    collection.load()

    organisation_uri = Organisation(
        organisation_path, Path(PIPELINE.path)
    ).organisation_uri
    patch = PIPELINE.patches(resource_hash)
    fieldnames = intermediary_fieldnames(SPECIFICATION, PIPELINE)

    pm = get_plugin_manager()

    harmoniser = Harmoniser(
        SPECIFICATION,
        PIPELINE,
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


@cli.command("transform", short_help="transform")
@input_output_path
@organisation_path
def transform_cmd(input_path, output_path, organisation_path):
    if not output_path:
        output_path = default_output_path_for("transformed", input_path)

    organisation = Organisation(organisation_path, Path(PIPELINE.path))
    schema = SPECIFICATION.pipeline[PIPELINE.name]["schema"]
    transformer = Transformer(
        schema,
        PIPELINE.transformations(),
        organisation.organisation,
    )
    stream = load_csv_dict(input_path)
    stream = transformer.transform(stream)
    save(stream, output_path, SPECIFICATION.current_fieldnames(schema))


@cli.command("load-entries", short_help="load entries")
@click.option("--output-path", type=click.Path(), default=None)
@click.argument("input-paths", nargs=-1, type=click.Path(exists=True))
def load_entries_cmd(input_paths, output_path):
    if not output_path:
        print("missing output path")
        sys.exit(2)

    repo = EntryRepository(output_path, create=True)
    loader = EntryLoader(repo)

    total = len(input_paths)
    for idx, path in enumerate(input_paths, start=1):
        logging.info("loading file %s of %s [%s]", idx, total, resource_hash_from(path))
        stream = load_csv_dict(path, include_line_num=True)
        loader.load(stream)


@cli.command("build-dataset", short_help="build dataset")
@input_output_path
def build_dataset_cmd(input_path, output_path):
    repo = EntryRepository(input_path)
    entities = repo.list_entities()
    logging.info("building dataset with %s entities", len(entities))
    schema = SPECIFICATION.pipeline[PIPELINE.name]["schema"]

    output = filter(
        lambda x: x["row"],
        (
            {
                "row": Entity(
                    repo.find_by_entity(entity),
                    schema,
                ).snapshot()
            }
            for entity in entities
        ),
    )

    save(
        output,
        output_path,
        SPECIFICATION.current_fieldnames(schema) + ["slug"],
    )


@cli.command("pipeline", short_help="convert, normalise, map, harmonise, transform")
@input_output_path
@collection_dir
@click.option(
    "--null-path",
    type=click.Path(exists=True),
    help="patterns for null fields",
    default=None,
)
@issue_dir
@organisation_path
@click.option("--save-harmonised", is_flag=True)
def pipeline_cmd(
    input_path,
    output_path,
    collection_dir,
    null_path,
    issue_dir,
    organisation_path,
    save_harmonised,
):
    resource_hash = resource_hash_from(input_path)
    organisation = Organisation(organisation_path, Path(PIPELINE.path))
    issues = Issues()

    schema = SPECIFICATION.pipeline[PIPELINE.name]["schema"]
    fieldnames = intermediary_fieldnames(SPECIFICATION, PIPELINE)
    patch = PIPELINE.patches(resource_hash)

    collection = Collection(name=None, directory=collection_dir)
    collection.load()
    line_converter = LineConverter()
    pm = get_plugin_manager()

    converter = Converter(PIPELINE.conversions())

    normaliser = Normaliser(PIPELINE.skip_patterns(resource_hash), null_path=null_path)
    mapper = Mapper(
        fieldnames,
        PIPELINE.columns(resource_hash),
        PIPELINE.concatenations(resource_hash),
    )
    harmoniser = Harmoniser(
        SPECIFICATION,
        PIPELINE,
        issues,
        collection,
        organisation.organisation_uri,
        patch,
        pm,
    )
    transformer = Transformer(
        SPECIFICATION.schema_field[schema],
        PIPELINE.transformations(),
        organisation.organisation,
    )

    key_field = SPECIFICATION.key_field(schema)

    slugger = Slugger(
        SPECIFICATION.pipeline[PIPELINE.name].get("slug-prefix", None),
        key_field,
        SPECIFICATION.pipeline[PIPELINE.name].get("scope-field", None),
    )

    pipeline_funcs = [
        converter.convert,
        normaliser.normalise,
        line_converter.convert,
        mapper.map,
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
                fieldnames=intermediary_fieldnames(SPECIFICATION, PIPELINE),
            )
            yield from output_tap

        pipeline_funcs.append(saver)

    pipeline_funcs = pipeline_funcs + [
        transformer.transform,
        slugger.slug,
    ]

    pipeline = compose(*pipeline_funcs)

    output = pipeline(input_path)

    save(
        output,
        output_path,
        fieldnames=SPECIFICATION.current_fieldnames(schema) + ["slug"],
    )

    issues_file = IssuesFile(path=os.path.join(issue_dir, resource_hash + ".csv"))
    issues_file.write_issues(issues)


# Endpoint commands


@cli.command(
    "collection-check-endpoints", short_help="check logs for failing endpoints"
)
@click.argument("first-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option(
    "--log-dir",
    type=click.Path(exists=True),
    help="path to log files",
    default="collection/log/",
)
@endpoint_path
@click.option(
    "--last-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="upper bound of date range to consider",
    default=str(date.today()),
)
def collection_check_endpoints_cmd(first_date, log_dir, endpoint_path, last_date):
    """find active endpoints that are failing during collection"""
    output = get_failing_endpoints_from_registers(
        log_dir, endpoint_path, first_date.date(), last_date.date()
    )
    print(canonicaljson.encode_canonical_json(output))


@cli.command(
    "collection-add-source",
    short_help="Add a new source to a collection",
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
)
@click.pass_context
@click.argument("collection", type=click.STRING)
@click.argument("endpoint-url", type=click.STRING)
@collection_dir
def collection_add_source_cmd(ctx, collection, endpoint_url, collection_dir):
    """
    followed by a sequence of optional name and value pairs including the following names:
    "attribution", "licence", "pipelines", "status", "plugin",
    "parameters", "start-date", "end-date"
    """
    print(ctx.args)
    if len(ctx.args) % 2:
        logging.error("odd number of name value pair arguments")
        sys.exit(2)
    entry = defaultdict(
        str,
        {ctx.args[i]: ctx.args[i + 1] for i in range(0, len(ctx.args), 2)},
    )
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


@cli.command("render")
@click.option("--dataset-path", required=True, type=click.Path())
@click.option("--key-fields", type=click.STRING, default=None)
@click.option("--local", is_flag=True)
@click.option("--limit", type=int, default=None)
def render_cmd(local, dataset_path, key_fields, limit):
    from digital_land_frontend.render import Renderer

    url_root = None
    if local:
        url_root = "/"

    # TODO: should probably read group_field from the specification
    group_field = "organisation"
    if PIPELINE.name in [
        "ancient-woodland",
        "area-of-outstanding-natural-beauty",
        "battlefield",
        "building-preservation-notice",
        "contribution-funding-status",
        "contribution-purpose",
        "developer-agreement-type",
        "developer-contribution-purpose",
        "development-plan-type",
        "development-policy-category",
        "document-type",
        "heritage-at-risk",
        "heritage-coast",
        "local-authority-district",
        "neighbourhood-plan-area",
        "ownership-status",
        "park-and-garden",
        "parish",
        "permitted-development-right",
        "planning-permission-status",
        "planning-permission-type",
        "protected-wreck-site",
        "scheduled-monument",
        "site-category",
        "site-of-special-scientific-interest",
        "world-heritage-site",
    ]:
        group_field = None

    schema = SPECIFICATION.pipeline[PIPELINE.name]["schema"]
    typology = SPECIFICATION.field_typology(PIPELINE.name)
    # TODO: should be the dataset name / slug-prefix here, not pipeline name ..
    renderer = Renderer(
        PIPELINE.name,
        schema,
        typology,
        SPECIFICATION.key_field(schema),
        url_root,
        group_field=group_field,
        limit=limit,
    )
    renderer.render_dataset(dataset_path)


def resource_hash_from(path):
    return Path(path).stem


def intermediary_fieldnames(specification, pipeline):
    schema = specification.pipeline[pipeline.name]["schema"]
    fieldnames = specification.schema_field[schema].copy()
    replacement_fields = list(pipeline.transformations().keys())
    for field in replacement_fields:
        if field in fieldnames:
            fieldnames.remove(field)
    return fieldnames


def default_output_path_for(command, input_path):
    return f"var/{command}/{resource_hash_from(input_path)}.csv"


def compose(*functions):
    def compose2(f, g):
        return lambda x: g(f(x))

    return functools.reduce(compose2, functions, lambda x: x)
