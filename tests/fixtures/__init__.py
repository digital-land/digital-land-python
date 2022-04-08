from datetime import date
from pathlib import Path
from shutil import copytree

import pytest


TODAY = date.today()


# Read-only fixtures
@pytest.fixture
def organisation_path(data_dir):
    return data_dir.joinpath("organisation.csv")


@pytest.fixture
def data_dir():
    return Path(__file__).parent.parent.joinpath("data").joinpath("listed-building")


# Copied fixtures
@pytest.fixture
def transformed_dir(data_dir, tmp_path):
    transformed_dir = tmp_path.joinpath("transformed")
    copytree(
        data_dir.joinpath("transformed"),
        transformed_dir,
    )
    return transformed_dir


@pytest.fixture
def dataset_dir(data_dir, tmp_path):
    dataset_dir = tmp_path.joinpath("dataset")
    copytree(
        data_dir.joinpath("dataset"),
        dataset_dir,
    )
    return dataset_dir


@pytest.fixture
def column_field_dir(data_dir, tmp_path):
    column_field_dir = tmp_path.joinpath("var").joinpath("column-field")
    copytree(
        data_dir.joinpath("var").joinpath("column-field"),
        column_field_dir,
        dirs_exist_ok=True,
    )
    return column_field_dir


@pytest.fixture
def dataset_resource_dir(data_dir, tmp_path):
    column_field_dir = tmp_path.joinpath("var").joinpath("dataset-resource")
    copytree(
        data_dir.joinpath("var").joinpath("dataset-resource"),
        column_field_dir,
        dirs_exist_ok=True,
    )
    return column_field_dir


@pytest.fixture
def pipeline_dir(data_dir, tmp_path):
    pipeline_dir = tmp_path.joinpath("pipeline")
    copytree(
        data_dir.joinpath("pipeline"),
        pipeline_dir,
        dirs_exist_ok=True,
    )
    return pipeline_dir


@pytest.fixture
def collection_resources_dir(data_dir, collection_dir):
    resources_dir = collection_dir.joinpath("resource")
    copytree(
        data_dir.joinpath("collection").joinpath("resources").joinpath("resource"),
        resources_dir,
    )
    return resources_dir


@pytest.fixture
def collection_metadata_dir(data_dir, collection_dir):
    copytree(
        data_dir.joinpath("collection").joinpath("csv"),
        collection_dir,
        dirs_exist_ok=True,
    )
    return collection_dir


@pytest.fixture
def collection_payload_dir(data_dir, collection_dir):
    log_dir = collection_dir.joinpath("log").joinpath(TODAY.isoformat())
    copytree(
        data_dir.joinpath("collection").joinpath("log"),
        log_dir,
        dirs_exist_ok=True,
    )
    return log_dir


# Blank output directory fixtures
@pytest.fixture
def collection_dir(tmp_path):
    collection_dir = tmp_path.joinpath("collection")
    collection_dir.mkdir()
    return collection_dir
