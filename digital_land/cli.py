import logging
import sys
from pathlib import Path

import click

from collections import defaultdict
from digital_land.collection import Collection
from digital_land.specification import Specification
from digital_land.configuration.main import Config
from digital_land.organisation import Organisation
from digital_land.commands import (
    add_redirections,
    assign_entities,
    fetch,
    collect,
    collection_list_resources,
    collection_pipeline_makerules,
    dataset_dump,
    dataset_dump_flattened,
    collection_save_csv,
    operational_issue_save_csv,
    convert,
    dataset_create,
    pipeline_run,
    collection_add_source,
    add_endpoints_and_lookups,
    collection_retire_endpoints_and_sources,
    organisation_create,
    organisation_check,
    save_state,
    check_state,
)

from digital_land.command_arguments import (
    collection_dir,
    config_collections_dir,
    operational_issue_dir,
    organisation_path,
    input_output_path,
    issue_dir,
    dataset_resource_dir,
    column_field_dir,
    converted_resource_dir,
    output_log_dir,
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

    if debug:
        logging.getLogger().setLevel(logging.DEBUG)


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


@cli.command(
    "operational-issue-save-csv", short_help="save Operational Issues as CSV package"
)
@operational_issue_dir
@click.pass_context
def operational_issue_save_csv_cmd(ctx, operational_issue_dir):
    dataset = ctx.obj["DATASET"]
    return operational_issue_save_csv(operational_issue_dir, dataset)


#
#  pipeline commands
#
@cli.command("convert", short_help="convert a resource to CSV")
@input_output_path
def convert_cmd(input_path, output_path):
    return convert(input_path, output_path)


@cli.command("dataset-create", short_help="create a dataset from processed resources")
@click.option("--output-path", type=click.Path(), default=None, help="sqlite3 path")
@organisation_path
@column_field_dir
@dataset_resource_dir
@issue_dir
@click.argument("input-paths", nargs=-1, type=click.Path(exists=True))
@click.pass_context
def dataset_create_cmd(
    ctx,
    input_paths,
    output_path,
    organisation_path,
    column_field_dir,
    dataset_resource_dir,
    issue_dir,
):
    return dataset_create(
        input_paths=input_paths,
        output_path=output_path,
        organisation_path=organisation_path,
        pipeline=ctx.obj["PIPELINE"],
        dataset=ctx.obj["DATASET"],
        specification=ctx.obj["SPECIFICATION"],
        column_field_dir=column_field_dir,
        dataset_resource_dir=dataset_resource_dir,
        issue_dir=issue_dir,
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
@click.option("--config-path", help="Path  to a configuration sqlite", default=None)
@click.option(
    "--resource",
    help="the resource hash to use if it can not be derived from filepath",
    default=None,
)
@input_output_path
@issue_dir
@column_field_dir
@dataset_resource_dir
@converted_resource_dir
@organisation_path
@collection_dir
@operational_issue_dir
@output_log_dir
@click.pass_context
def pipeline_command(
    ctx,
    input_path,
    output_path,
    issue_dir,
    column_field_dir,
    dataset_resource_dir,
    converted_resource_dir,
    organisation_path,
    save_harmonised,
    endpoints,
    organisations,
    entry_date,
    custom_temp_dir,
    collection_dir,
    operational_issue_dir,
    config_path,
    resource,
    output_log_dir,
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
        collection_dir=collection_dir,
        issue_dir=issue_dir,
        operational_issue_dir=operational_issue_dir,
        column_field_dir=column_field_dir,
        dataset_resource_dir=dataset_resource_dir,
        converted_resource_dir=converted_resource_dir,
        organisation_path=organisation_path,
        save_harmonised=save_harmonised,
        endpoints=endpoints,
        organisations=organisations,
        entry_date=entry_date,
        custom_temp_dir=custom_temp_dir,
        config_path=config_path,
        resource=resource,
        output_log_dir=output_log_dir,
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


@cli.command(
    "expectations-dataset-checkpoint",
    short_help="runs data quality expectations against a dataset sqlite file",
)
@click.option(
    "--dataset",
    type=click.STRING,
    help="the dataset which is stored in the file path, the checkpoint is ran for one dataset at a time",
    required=True,
)
@click.option(
    "--file-path",
    type=click.Path(),
    help="path to the sqlite3 dataset that contains the dataset data",
    required=True,
)
@click.option(
    "--log-dir",
    type=click.Path(),
    help="directory to store expectation logs. an expectation directoy will be ceated here",
    required=True,
)
@click.option(
    "--configuration-path",
    type=click.Path(),
    help="path to the configuration sqlite file",
    required=True,
)
@click.option(
    "--organisation-path",
    type=click.Path(),
    help="path to the organisation data for the organisation class",
    required=True,
)
@click.option(
    "--specification-dir",
    type=click.Path(),
    help="directory  containing the specification",
    required=True,
)
def expectations_run_dataset_checkpoint(
    dataset,
    file_path,
    log_dir,
    configuration_path,
    organisation_path,
    specification_dir,
):
    from digital_land.expectations.commands import run_dataset_checkpoint

    specification = Specification(specification_dir)
    output_dir = Path(log_dir) / "expectation"
    config = Config(path=configuration_path, specification=specification)
    organisations = Organisation(organisation_path=organisation_path)
    logging.error(type(dataset))
    run_dataset_checkpoint(dataset, file_path, output_dir, config, organisations)


@cli.command(
    "expectations-converted-resource-checkpoint",
    short_help="runs data quality expectations against a converted resource",
)
@click.option(
    "--data-path", help="path to the converted resource to use", required=True
)
@click.option("--output-dir", help="path/name to sqlite3 dataset", required=True)
@click.option("--specification-dir", help="checkpoint to run", required=True)
@click.option("--dataset", help="checkpoint to run", required=True)
def expectations_run_converted_resource_checkpoint(
    data_path, output_dir, specification_dir, dataset
):
    from digital_land.expectations.commands import run_converted_resource_checkpoint

    spec = Specification(specification_dir)
    typology = spec.get_dataset_typology(dataset)
    run_converted_resource_checkpoint(data_path, output_dir, dataset, typology)


@cli.command("retire-endpoints-and-sources")
@config_collections_dir
@click.argument("csv-path", nargs=1, type=click.Path())
def retire_endpoints_cmd(config_collections_dir, csv_path):
    return collection_retire_endpoints_and_sources(config_collections_dir, csv_path)


# edit to add collection_name in
@cli.command("add-endpoints-and-lookups")
@click.argument("csv-path", nargs=1, type=click.Path())
@click.argument("collection-name", nargs=1, type=click.Path())
@collection_dir
@organisation_path
@click.option(
    "--specification-dir", "-s", type=click.Path(exists=True), default="specification/"
)
@click.option("--pipeline-dir", "-p", type=click.Path(exists=True), default="pipeline/")
def add_endpoint_and_lookups_cmd(
    csv_path,
    collection_name,
    collection_dir,
    specification_dir,
    pipeline_dir,
    organisation_path,
):
    """
    adds new resources to the collection, based on records in csv_path
    :param ctx:
    :param csv_path:
    :param collection_dir:
    :return:
    """
    csv_file_path = Path(csv_path)
    if not csv_file_path.is_file():
        logging.error("no csv file was provided")
        sys.exit(2)

    return add_endpoints_and_lookups(
        csv_file_path,
        collection_name,
        collection_dir,
        pipeline_dir,
        specification_dir,
        organisation_path,
    )


@cli.command("assign-entities")
@click.argument("resource-path", nargs=1, type=click.Path())
@click.argument("endpoints", nargs=1, type=click.Path())
@click.argument("collection-name", nargs=1, type=click.Path())
@click.argument("dataset", nargs=1, type=click.Path())
@click.argument("organisation", nargs=1, type=click.Path())
@collection_dir
@organisation_path
@click.option(
    "--specification-dir", "-s", type=click.Path(exists=True), default="specification/"
)
@click.option("--pipeline-dir", "-p", type=click.Path(exists=True), default="pipeline/")
def assign_entities_cmd(
    resource_path,
    endpoints,
    collection_name,
    dataset,
    organisation,
    collection_dir,
    specification_dir,
    pipeline_dir,
    organisation_path,
):
    """
    Assigns entities for given resource in collection assuming it's endpoint has already been added to collection
    :param resource_path:
    :param collection_name:
    :return:
    """
    resource_file_path = Path(resource_path)
    if not resource_file_path.is_file():
        logging.error("resource file not found")
        sys.exit(2)

    # Load collection
    collection = Collection(name=collection_name, directory=collection_dir)
    collection.load()

    return assign_entities(
        [resource_file_path],
        collection,
        dataset,
        [organisation],
        pipeline_dir,
        specification_dir,
        organisation_path,
        [endpoints],
    )


@cli.command("add-redirections")
@click.argument("csv_path", nargs=1, type=click.Path())
@click.option("--pipeline-dir", "-p", type=click.Path(exists=True), default="pipeline/")
def add_redirections_cmd(csv_path, pipeline_dir):
    """
    Add redirections to the old-entity file based on records in csv_path
    :param resource_path:
    :param collection_name:
    :return:
    """

    csv_file_path = Path(csv_path)
    if not csv_file_path.is_file():
        logging.error("no csv file was provided")
        sys.exit(2)

    return add_redirections(csv_file_path, pipeline_dir)


@cli.command("organisation-create", short_help="create the organisation.csv file")
@click.option(
    "--flattened-dir",
    type=click.Path(exists=True),
    help="Directory of flattened files.",
)
@click.option(
    "--dataset-dir",
    type=click.Path(exists=True),
    help="Directory of dataset files.",
)
@click.option(
    "--download-url",
    type=click.STRING,
    help="URL to downlaod dataset from",
)
@click.option(
    "--cache-dir",
    type=click.Path(),
    default="var/cache/organisation-collection/dataset/",
    help="Cache directory for downloaded files.",
)
@click.option(
    "--specification-dir",
    "-s",
    type=click.Path(exists=True),
    default="specification/",
    help="Directory of specification files.",
)
@click.option("--output-path", type=click.Path(), default=None, help="Output CSV path.")
def organisation_create_cmd(
    flattened_dir, dataset_dir, specification_dir, download_url, cache_dir, output_path
):
    return organisation_create(
        specification_dir=specification_dir,
        flattened_dir=flattened_dir,
        dataset_dir=dataset_dir,
        download_url=download_url,
        cache_dir=cache_dir,
        path=output_path,
    )


@cli.command("organisation-check", short_help="check the organisation.csv file")
@click.option("--input-path", type=click.Path(), default=None, help="Input CSV path.")
@click.option(
    "--specification-dir",
    "-s",
    type=click.Path(exists=True),
    default="specification/",
    help="Directory of specification files.",
)
@click.option(
    "--lpa-path",
    type=click.Path(),
    default="var/cache/local-planning-authority.csv",
    help="Path of LPA CSV path.",
)
@click.option(
    "--output-path",
    type=click.Path(),
    default="dataset/organisation-check.csv",
    help="Output CSV path.",
)
def organisation_check_cmd(input_path, specification_dir, lpa_path, output_path):
    return organisation_check(
        path=input_path,
        specification_dir=specification_dir,
        lpa_path=lpa_path,
        output_path=output_path,
    )


@cli.command("config-create", short_help="create a dataset from processed resources")
@click.option("--config-path", type=click.Path(), default=None, help="sqlite3 path")
@click.pass_context
def config_create_cmd(ctx, config_path):
    """
    A function which builds an empty configuration database based on the spec
    """
    config = Config(path=config_path, specification=ctx.obj["SPECIFICATION"])
    config.create()


@cli.command("config-load", short_help="create a dataset from processed resources")
@click.option("--config-path", type=click.Path(), default=None, help="sqlite3 path")
@click.pass_context
def config_load_cmd(ctx, config_path):

    config = Config(path=config_path, specification=ctx.obj["SPECIFICATION"])
    tables = {key: ctx.obj["PIPELINE"].path for key in config.tables.keys()}
    config.load(tables)


@cli.command("save-state", short_help="save a state file")
@click.option(
    "--specification-dir",
    type=click.Path(),
    default="specification",
    help="directory containing the specification",
)
@click.option(
    "--collection-dir",
    type=click.Path(),
    default="collection",
    help="directory containing the collection",
)
@click.option(
    "--pipeline-dir",
    type=click.Path(),
    default="pipeline",
    help="directory containing the pipeline",
)
@click.option(
    "--output-path",
    "-o",
    type=click.Path(),
    default="state.json",
    help="path of the output state file",
)
def save_state_cmd(specification_dir, collection_dir, pipeline_dir, output_path):
    save_state(specification_dir, collection_dir, pipeline_dir, output_path)


@cli.command(
    "check-state",
    short_help="compare the current state against a stated file. Returns with a non-zero return code if they differ.",
)
@click.option(
    "--specification-dir",
    type=click.Path(),
    default="specification",
    help="directory containing the specification",
)
@click.option(
    "--collection-dir",
    type=click.Path(),
    default="collection",
    help="directory containing the collection",
)
@click.option(
    "--pipeline-dir",
    type=click.Path(),
    default="pipeline",
    help="directory containing the pipeline",
)
@click.option(
    "--state-path",
    type=click.Path(),
    default="state.json",
    help="path of the output state file",
)
def check_state_cmd(specification_dir, collection_dir, pipeline_dir, state_path):
    # If the state isn't the same, use a non-zero return code so scripts can
    # detect this, and print a message. If it is the same, exit silenty wirh a
    # 0 retun code.
    if not check_state(specification_dir, collection_dir, pipeline_dir, state_path):
        print(f"State differs from {state_path}")
        sys.exit(1)
