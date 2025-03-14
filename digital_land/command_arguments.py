# Custom decorators for common command arguments
import functools

import click


def input_output_path(f):
    arguments = [
        click.argument("input-path", type=click.Path(exists=True)),
        click.argument("output-path", type=click.Path(), default=""),
    ]
    return functools.reduce(lambda x, arg: arg(x), reversed(arguments), f)


def collection_dir(f):
    return click.option(
        "--collection-dir",
        "-c",
        type=click.Path(exists=True),
        default="collection/",
    )(f)


def config_collections_dir(f):
    return click.option(
        "--config-collections-dir",
        type=click.Path(exists=True),
        default="collection/",
    )(f)


def issue_dir(f):
    return click.option(
        "--issue-dir", "-i", type=click.Path(exists=True), default="issue/"
    )(f)


def operational_issue_dir(f):
    return click.option(
        "--operational-issue-dir",
        "-i",
        type=click.Path(),
        default="performance/operational_issue/",
    )(f)


def column_field_dir(f):
    return click.option(
        "--column-field-dir",
        type=click.Path(exists=True),
        default="var/column-field/",
    )(f)


def output_log_dir(f):
    return click.option(
        "--output-log-dir", "-i", type=click.Path(exists=True), default="log/"
    )(f)


def dataset_resource_dir(f):
    return click.option(
        "--dataset-resource-dir",
        type=click.Path(exists=True),
        default="var/dataset-resource/",
    )(f)


def converted_resource_dir(f):
    return click.option(
        "--converted-resource-dir",
        type=click.Path(exists=True),
        default="var/converted-resource/",
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
