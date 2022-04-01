import functools
import logging
import sys
from collections import defaultdict

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


def dataset(f):
    return click.option("--dataset", "-n", type=click.STRING)(f)


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


def column_field_dir(f):
    return click.option(
        "--column-field-dir",
        type=click.Path(exists=True),
        default="var/column-field/",
    )(f)


def dataset_resource_dir(f):
    return click.option(
        "--dataset-resource-dir",
        type=click.Path(exists=True),
        default="var/dataset-resource/",
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


@click.group()
@click.option("-d", "--debug/--no-debug", default=False)
@dataset
@pipeline_dir
@specification_dir
def cli(debug, dataset, pipeline_dir, specification_dir):
    global API
    API = DigitalLandApi(debug, dataset, pipeline_dir, specification_dir)


@cli.command("fetch")
@click.argument("url")
def fetch_cmd(url):
    """fetch resource from a single endpoint"""
    return API.fetch_cmd(url)


@cli.command("collect")
@click.argument(
    "endpoint-path",
    type=click.Path(exists=True),
    default="collection/endpoint.csv",
)
@collection_dir
def collect_cmd(endpoint_path, collection_dir):
    """fetch resources from collection endpoints"""
    return API.collect_cmd(endpoint_path, collection_dir)


#
#  collection commands
#  TBD: make sub commands
#
@cli.command("collection-list-resources", short_help="list resources for a pipeline")
@collection_dir
def collection_list_resources_cmd(collection_dir):
    return API.collection_list_resources_cmd(collection_dir)


@cli.command(
    "collection-pipeline-makerules",
    short_help="generate makerules for processing a collection",
)
@collection_dir
def collection_pipeline_makerules_cmd(collection_dir):
    return API.collection_pipeline_makerules_cmd(collection_dir)


@cli.command("collection-save-csv", short_help="save collection as CSV package")
@collection_dir
def collection_save_csv_cmd(collection_dir):
    return API.collection_save_csv_cmd(collection_dir)


#
#  pipeline commands
#
@cli.command("convert", short_help="convert a resource to CSV")
@input_output_path
def convert_cmd(input_path, output_path):
    return API.convert_cmd(input_path, output_path)


@cli.command("dataset-create", short_help="create a dataset from processed resources")
@click.option("--output-path", type=click.Path(), default=None, help="sqlite3 path")
@click.argument("input-paths", nargs=-1, type=click.Path(exists=True))
def dataset_create_cmd(input_paths, output_path):
    return API.dataset_create_cmd(input_paths, output_path)


@cli.command("dataset-entries", short_help="dump dataset entries as csv")
@input_output_path
def dataset_dump_cmd(input_path, output_path):
    API.dataset_dump_cmd(input_path, output_path)


@cli.command(
    "dataset-entries-hoisted",
    short_help="dump dataset entries as csv with additional top-level `entity.json` fields",
)
@input_output_path
def dataset_dump_hoisted_cmd(input_path, output_path):
    API.dataset_dump_hoisted_cmd(csv_path=input_path, hoisted_csv_path=output_path)


@cli.command("pipeline", short_help="process a resource")
@input_output_path
@collection_dir
@click.option(
    "--null-path",
    type=click.Path(exists=True),
    help="patterns for null fields",
    default=None,
)
@issue_dir
@click.option("--save-harmonised", is_flag=True)
@column_field_dir
@dataset_resource_dir
def pipeline_cmd_API(
    input_path,
    output_path,
    collection_dir,
    null_path,
    issue_dir,
    save_harmonised,
    column_field_dir,
    dataset_resource_dir,
):
    return API.pipeline_cmd(
        input_path,
        output_path,
        collection_dir,
        null_path,
        issue_dir,
        save_harmonised,
        column_field_dir,
        dataset_resource_dir,
    )


# Endpoint commands


@cli.command(
    "collection-add-source",
    short_help="Add a new source and endpoint to a collection",
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
