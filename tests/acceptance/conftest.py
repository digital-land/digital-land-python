import os
import urllib.request
import pandas as pd

import pytest

from pathlib import Path
from urllib.error import URLError


test_pipeline = "national-park"
test_resource = "5158d13bfc6f0723b1fb07c975701a906e83a1ead4aee598ee34e241c79a5f3d"
test_endpoint = "d779ad1c91c5a46e2d4ace4d5446d7d7f81df1ed058f882121070574697a5412"


# Do we need all this setup?
# Need to revisit this and remove parts we don't need
@pytest.fixture(scope="session")
def test_dirs(tmp_path_factory):
    """
    This fixture prepares the folders and data required by the tests that use it.
    Some of the data is retrieved remotely, so it makes sense to retrieve once only
    for all tests.
    :param tmp_path_factory:
    :return: dict of Path (directory paths)
    """
    # directories
    pipeline_dir = tmp_path_factory.mktemp("async_pipeline", numbered=False)

    specification_dir = tmp_path_factory.mktemp("async_specification", numbered=False)
    transformed_dir = tmp_path_factory.mktemp("async_transformed", numbered=False)

    collection_dir = tmp_path_factory.mktemp("async_collection", numbered=False)
    issues_log_dir = tmp_path_factory.mktemp("async_issues-log", numbered=False)
    operational_issues_dir = tmp_path_factory.mktemp(
        "async_operational_issues", numbered=False
    )
    datasource_log_dir = tmp_path_factory.mktemp("async_datasource-log", numbered=False)
    dataset_resource_dir = tmp_path_factory.mktemp(
        "async_dataset-resource", numbered=False
    )
    converted_resource_dir = tmp_path_factory.mktemp(
        "async_converted-resource", numbered=False
    )

    column_field_dir = tmp_path_factory.mktemp("column-field", numbered=False)

    cache_dir = tmp_path_factory.mktemp("cache", numbered=False)

    custom_temp_dir = tmp_path_factory.mktemp("tmp_dir", numbered=False)

    output_dir = tmp_path_factory.mktemp("async_output", numbered=False)

    # data - pipeline
    raw_data = get_pipeline_csv_data_with_resources_and_endpoints(
        test_pipeline, test_resource, test_endpoint
    )
    columns_data = pd.DataFrame.from_dict(raw_data)
    columns_data.to_csv(f"{pipeline_dir}/column.csv", index=False)

    # data - specification
    copy_latest_specification_files_to(specification_dir)

    # data - collection
    generate_test_collection_files(
        str(collection_dir), test_pipeline, test_resource, test_endpoint
    )

    return {
        "collection_dir": collection_dir,
        "output_dir": output_dir,
        "pipeline_dir": pipeline_dir,
        "column_field_dir": column_field_dir,
        "specification_dir": specification_dir,
        "transformed_dir": transformed_dir,
        "issues_log_dir": issues_log_dir,
        "operational_issues_dir": operational_issues_dir,
        "datasource_log_dir": datasource_log_dir,
        "dataset_resource_dir": dataset_resource_dir,
        "converted_resource_dir": converted_resource_dir,
        "cache_dir": cache_dir,
        "custom_temp_dir": custom_temp_dir,
    }


def get_pipeline_csv_data_with_resources_and_endpoints(
    pipeline: str = "", resource: str = "", endpoint: str = ""
):
    return {
        "dataset": [pipeline, pipeline, pipeline],
        "resource": [
            "",
            resource,
            "",
        ],
        "endpoint": [
            "",
            "",
            endpoint,
        ],
        "column": [
            "NAME",
            "res-col-one",
            "ep-col-one",
        ],
        "field": [
            "name",
            "res_field_one",
            "ep_field_one",
        ],
    }


def copy_latest_specification_files_to(specification_dir: Path):
    error_msg = "Failed to download specification files"
    if not specification_dir:
        pytest.fail(error_msg)

    url_domain = "https://raw.githubusercontent.com"
    url_path = "/digital-land/specification/main/specification"
    specification_url = f"{url_domain}{url_path}"
    specification_csv_list = [
        "attribution.csv",
        "collection.csv",
        "datapackage-dataset.csv",
        "datapackage.csv",
        "dataset-field.csv",
        "dataset-schema.csv",
        "dataset.csv",
        "datatype.csv",
        "field.csv",
        "issue-type.csv",
        "licence.csv",
        "organisation-dataset.csv",
        "pipeline.csv",
        "prefix.csv",
        "project-status.csv",
        "project.csv",
        "provision-reason.csv",
        "provision-rule.csv",
        "schema-field.csv",
        "schema.csv",
        "severity.csv",
        "specification-status.csv",
        "specification.csv",
        "theme.csv",
        "typology.csv",
    ]

    try:
        for specification_csv in specification_csv_list:
            urllib.request.urlretrieve(
                f"{specification_url}/{specification_csv}",
                os.path.join(specification_dir, specification_csv),
            )
    except URLError:
        pytest.fail(error_msg)


def generate_test_collection_files(
    collection_dir: str = "", pipeline: str = "", resource: str = "", endpoint: str = ""
):
    collection_csv_list = [
        "endpoint.csv",
        "source.csv",
        "resource.csv",
        f"{resource}.csv",
    ]

    for collection_csv in collection_csv_list:
        if collection_csv == "endpoint.csv":
            raw_data = {
                "endpoint": [endpoint],
                "endpoint-url": ["https://example.com/register/1.csv"],
                "start-date": ["2020-01-12"],
                "end-date": ["2020-02-01"],
            }

        if collection_csv == "source.csv":
            raw_data = {
                "source": [""],
                "collection": ["test-collection"],
                "pipeline": [pipeline],
                "organisation": ["test-org"],
                "endpoint": [endpoint],
                "documentation-url": [""],
                "licence": [""],
                "attribution": [""],
                "start-date": [""],
                "end-date": [""],
            }

        if collection_csv == "resource.csv":
            raw_data = {
                "resource": [resource],
                "bytes": [1234],
                "organisations": ["test-org"],
                "datasets": [pipeline],
                "endpoints": [endpoint],
                "res-col-one": ["res-col-val"],
                "ep-col-one": ["ep-col-val"],
                "start-date": ["2020-01-12"],
                "end-date": ["2020-02-01"],
            }

        if collection_csv.startswith(resource):
            raw_data = {
                "resource": [resource],
                "bytes": [1234],
                "organisations": ["test-org"],
                "datasets": [pipeline],
                "endpoints": [endpoint],
                "res-col-one": ["res-col-val"],
                "ep-col-one": ["ep-col-val"],
                "start-date": ["2020-01-12"],
                "end-date": ["2020-02-01"],
            }

        columns_data = pd.DataFrame.from_dict(raw_data)
        columns_data.to_csv(f"{collection_dir}/{collection_csv}", index=False)
