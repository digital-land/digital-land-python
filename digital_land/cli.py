import functools
import logging
import sys
from collections import defaultdict
from datetime import date

import click

from digital_land.api import DigitalLandApi

# pyright: reportGeneralTypeIssues=false
API: DigitalLandApi = None


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
    global API
    API = DigitalLandApi(debug, pipeline_name, pipeline_dir, specification_dir)


@cli.command("fetch")
@click.argument("url")
def fetch_cmd(url):
    """fetch a single source endpoint URL, and add it to the collection"""
    return API.fetch_cmd(url)


@cli.command("collect")
@click.argument(
    "endpoint-path",
    type=click.Path(exists=True),
    default="collection/endpoint.csv",
)
@collection_dir
def collect_cmd(endpoint_path, collection_dir):
    """fetch the sources listed in the endpoint-url column of the ENDPOINT_PATH CSV file"""
    return API.collect_cmd(endpoint_path, collection_dir)


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
    return API.index_cmd()


@cli.command("collection-list-resources", short_help="list resources for a pipeline")
@collection_dir
def pipeline_collection_list_resources_cmd(collection_dir):
    return API.pipeline_collection_list_resources_cmd(collection_dir)


@cli.command(
    "collection-pipeline-makerules",
    short_help="generate pipeline makerules for a collection",
)
@collection_dir
def pipeline_collection_pipeline_makerules_cmd(collection_dir):
    return API.pipeline_collection_pipeline_makerules_cmd(collection_dir)


@cli.command("collection-save-csv", short_help="save collection as CSV package")
@collection_dir
def pipeline_collection_save_csv_cmd(collection_dir):
    return API.pipeline_collection_save_csv_cmd(collection_dir)


#
#  pipeline commands
#
@cli.command("convert", short_help="convert to a well-formed, UTF-8 encoded CSV file")
@input_output_path
def convert_cmd(input_path, output_path):
    return API.convert_cmd(input_path, output_path)


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
    return API.normalise_cmd(input_path, output_path, null_path, skip_path)


@cli.command("map", short_help="map misspelt column names to those in a pipeline")
@input_output_path
def map_cmd(input_path, output_path):
    return API.map_cmd(input_path, output_path)


@cli.command("filter", short_help="filter out rows by field values")
@input_output_path
def filter_cmd(input_path, output_path):
    return API.filter_cmd(input_path, output_path)


@cli.command(
    "harmonise",
    short_help="strip whitespace and null fields, remove blank rows and columns",
)
@input_output_path
@issue_dir
@organisation_path
def harmonise_cmd(input_path, output_path, issue_dir, organisation_path):
    return API.harmonise_cmd(input_path, output_path, issue_dir, organisation_path)


@cli.command("transform", short_help="transform")
@input_output_path
@organisation_path
def transform_cmd(input_path, output_path, organisation_path):
    return API.transform_cmd(input_path, output_path, organisation_path)


@cli.command("load-entries", short_help="load entries")
@click.option("--output-path", type=click.Path(), default=None)
@click.argument("input-paths", nargs=-1, type=click.Path(exists=True))
def load_entries_cmd(input_paths, output_path):
    return API.load_entries_cmd(input_paths, output_path)


@cli.command("build-dataset", short_help="build dataset")
@input_output_path
def build_dataset_cmd(input_path, output_path):
    return API.build_dataset_cmd(input_path, output_path)


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
    return API.pipeline_cmd(
        input_path,
        output_path,
        collection_dir,
        null_path,
        issue_dir,
        organisation_path,
        save_harmonised,
    )


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
    return API.collection_check_endpoints_cmd(
        first_date, log_dir, endpoint_path, last_date
    )


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
    if len(ctx.args) % 2:
        logging.error("odd number of name value pair arguments")
        sys.exit(2)
    entry = defaultdict(
        str,
        {ctx.args[i]: ctx.args[i + 1] for i in range(0, len(ctx.args), 2)},
    )
    return API.collection_add_source_cmd(
        entry, collection, endpoint_url, collection_dir
    )


@cli.command("build-datasette", short_help="build docker image for datasette")
@click.option("--tag", "-t", default="data")
@click.option("--data-dir", default="./var/cache")
@click.option("--ext", default="sqlite3")
@click.option("--options", default=None)
def build_datasette(tag, data_dir, ext, options):
    return API.build_datasette(tag, data_dir, ext, options)
