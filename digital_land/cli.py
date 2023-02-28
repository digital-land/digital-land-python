import logging
import sys
import click

from collections import defaultdict

from digital_land.commands import (
    fetch,
    collect,
    collection_list_resources,
    collection_pipeline_makerules,
    dataset_dump,
    dataset_dump_flattened,
    collection_save_csv,
    convert,
    dataset_create,
    pipeline_run,
    collection_add_source,
    expectations,
)

from digital_land.command_arguments import (
    collection_dir,
    organisation_path,
    input_output_path,
    issue_dir,
    dataset_resource_dir,
    column_field_dir,
)


@click.group()
@click.option("-d", "--debug/--no-debug", type=click.BOOL, default=False)
@click.option("--dataset", "-n", type=click.STRING)
@click.option("--pipeline-dir", "-p", type=click.Path(), default="pipeline/")
@click.option(
    "--specification-dir", "-s", type=click.Path(exists=True), default="specification/"
)
@click.pass_context
def cli(ctx, debug, dataset, pipeline_dir, specification_dir):
    ctx.ensure_object(dict)

    from digital_land.pipeline import Pipeline
    from digital_land.specification import Specification

    ctx.obj["PIPELINE"] = Pipeline(pipeline_dir, dataset)
    ctx.obj["SPECIFICATION"] = Specification(specification_dir)
    ctx.obj["DATASET"] = dataset
    ctx.obj["DEBUG"] = debug


@cli.command("fetch")
@click.argument("url")
@click.pass_context
def fetch_cmd(ctx, url):
    """fetch resource from a single endpoint"""
    return fetch(url, ctx.obj["PIPELINE"])


@cli.command("collect")
@click.argument(
    "endpoint-path",
    type=click.Path(exists=True),
    default="collection/endpoint.csv",
)
@collection_dir
@click.pass_context
def collect_cmd(ctx, endpoint_path, collection_dir):
    """fetch resources from collection endpoints"""
    return collect(endpoint_path, collection_dir, ctx.obj["PIPELINE"])


#
#  collection commands
#
@cli.command("collection-list-resources", short_help="list resources for a pipeline")
@collection_dir
def collection_list_resources_cmd(collection_dir):
    return collection_list_resources(collection_dir)


@cli.command(
    "collection-pipeline-makerules",
    short_help="generate makerules for processing a collection",
)
@collection_dir
def collection_pipeline_makerules_cmd(collection_dir):
    return collection_pipeline_makerules(collection_dir)


@cli.command("collection-save-csv", short_help="save collection as CSV package")
@collection_dir
def collection_save_csv_cmd(collection_dir):
    return collection_save_csv(collection_dir)


#
#  pipeline commands
#
@cli.command("convert", short_help="convert a resource to CSV")
@input_output_path
def convert_cmd(input_path, output_path):
    return convert(input_path, output_path)


@cli.command("dataset-create", short_help="create a dataset from processed resources")
@click.option("--output-path", type=click.Path(), default=None, help="sqlite3 path")
@click.argument("input-paths", nargs=-1, type=click.Path(exists=True))
@organisation_path
@click.pass_context
def dataset_create_cmd(ctx, input_paths, output_path, organisation_path):
    return dataset_create(
        input_paths=input_paths,
        output_path=output_path,
        organisation_path=organisation_path,
        pipeline=ctx.obj["PIPELINE"],
        dataset=ctx.obj["DATASET"],
        specification=ctx.obj["SPECIFICATION"],
    )


@cli.command("dataset-entries", short_help="dump dataset entries as csv")
@input_output_path
def dataset_dump_cmd(input_path, output_path):
    dataset_dump(input_path, output_path)


@cli.command(
    "dataset-entries-flattened",
    short_help="dump dataset entries as csv with additional top-level `entity.json` fields",
)
@input_output_path
@click.pass_context
def dataset_dump_flattened_cmd(ctx, input_path, output_path):
    specification = ctx.obj["SPECIFICATION"]
    dataset = ctx.obj["DATASET"]
    dataset_dump_flattened(input_path, output_path, specification, dataset)


@cli.command("pipeline", short_help="process a resource")
@click.option("--save-harmonised", is_flag=True)
@click.option("--endpoints", help="list of endpoint hashes", default="")
@click.option("--organisations", help="list of organisations", default="")
@click.option("--entry-date", help="default entry-date value", default="")
@click.option("--custom-temp-dir", help="default temporary directory", default=None)
@input_output_path
@issue_dir
@column_field_dir
@dataset_resource_dir
@organisation_path
@click.pass_context
def pipeline_command(
    ctx,
    input_path,
    output_path,
    issue_dir,
    column_field_dir,
    dataset_resource_dir,
    organisation_path,
    save_harmonised,
    endpoints,
    organisations,
    entry_date,
    custom_temp_dir,
):
    dataset = ctx.obj["DATASET"]
    pipeline = ctx.obj["PIPELINE"]
    specification = ctx.obj["SPECIFICATION"]

    endpoints = endpoints.split()
    organisations = organisations.split()

    return pipeline_run(
        dataset,
        pipeline,
        specification,
        input_path,
        output_path,
        issue_dir=issue_dir,
        column_field_dir=column_field_dir,
        dataset_resource_dir=dataset_resource_dir,
        organisation_path=organisation_path,
        save_harmonised=save_harmonised,
        endpoints=endpoints,
        organisations=organisations,
        entry_date=entry_date,
        custom_temp_dir=custom_temp_dir,
    )


# Endpoint commands


@cli.command(
    "collection-add-source",
    short_help="Add a new source and endpoint to a collection",
    context_settings=dict(ignore_unknown_options=True, allow_extra_args=True),
)
@click.argument("collection", type=click.STRING)
@click.argument("endpoint-url", type=click.STRING)
@collection_dir
@click.pass_context
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
    return collection_add_source(entry, collection, endpoint_url, collection_dir)


@cli.command("expectations", short_help="runs data quality expectations")
@click.option("--results-path", help="path to save json results", required=True)
@click.option(
    "--sqlite-dataset-path", help="path/name to sqlite3 dataset", required=True
)
@click.option("--data-quality-yaml", help="path to expectations yaml", required=True)
def call_expectations(results_path, sqlite_dataset_path, data_quality_yaml):
    return expectations(results_path, sqlite_dataset_path, data_quality_yaml)
