import csv
import logging
import os
import tempfile
from unittest.mock import Mock
import pytest
from requests import HTTPError

from digital_land.commands import validate_and_add_data_input
from tests.acceptance.conftest import copy_latest_specification_files_to

# import from digital_land validate_and_add_data_input_error_thrown_when_no_resource_downloaded


@pytest.fixture(scope="module")
def specification_dir(tmp_path_factory):
    specification_dir = tmp_path_factory.mktemp("specification")
    copy_latest_specification_files_to(specification_dir)
    return specification_dir


@pytest.fixture(scope="function")
def collection_dir(tmp_path_factory):
    collection_dir = tmp_path_factory.mktemp("collection")

    # create source csv
    source_fieldnames = [
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "licence",
        "organisation",
        "pipelines",
        "entry-date",
        "start-date",
        "end-date",
    ]

    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=source_fieldnames)
        dictwriter.writeheader()

    # create endpoint csv
    endpoint_fieldnames = [
        "endpoint",
        "endpoint-url",
        "parameters",
        "plugin",
        "entry-date",
        "start-date",
        "end-date",
    ]

    with open(os.path.join(collection_dir, "endpoint.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=endpoint_fieldnames)
        dictwriter.writeheader()
    return collection_dir


@pytest.fixture(scope="module")
def organisation_csv():
    organisation_path = tempfile.NamedTemporaryFile().name
    organisation_fieldnames = [
        "dataset",
        "end-date",
        "entity",
        "entry-date",
        "name",
        "organisation",
        "prefix",
        "reference",
        "start-date",
    ]
    organisation_row = {
        "dataset": "local-authority",
        "end-date": "",
        "entity": 314,
        "entry-date": "2023-11-19",
        "name": "South Staffordshire Council",
        "organisation": "local-authority:SST",
        "prefix": "local-authority",
        "reference": "SST",
        "start-date": "",
    }

    with open(organisation_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=organisation_fieldnames)
        writer.writeheader()
        writer.writerow(organisation_row)

    return organisation_path


@pytest.fixture
def mock_request_get(mocker):
    data = {"reference": "1", "value": "test"}
    csv_content = str(data).encode("utf-8")

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.request.headers = {"test": "test"}
    mock_response.headers = {"test": "test"}
    mock_response.content = csv_content
    mocker.patch(
        "requests.Session.get",
        return_value=mock_response,
    )


def create_input_csv(
    data,
    fieldnames=[
        "organisation",
        "documentation-url",
        "endpoint-url",
        "start-date",
        "pipelines",
        "plugin",
        "licence",
    ],
):
    tmp_input_path = tempfile.NamedTemporaryFile().name

    with open(tmp_input_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(data)

    return tmp_input_path


def test_validate_and_add_data_input_no_error(
    collection_dir,
    specification_dir,
    organisation_csv,
    caplog,
    mock_request_get,
):
    collection_name = "conservation-area"
    no_error_input_data = {
        "organisation": "local-authority:SST",
        "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
        "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
        "start-date": "",
        "pipelines": "conservation-area",
        "plugin": "",
        "licence": "ogl3",
    }

    tmp_input_path = create_input_csv(no_error_input_data)

    with caplog.at_level(logging.ERROR):
        validate_and_add_data_input(
            tmp_input_path,
            collection_name,
            collection_dir,
            specification_dir,
            organisation_csv,
        )
        assert len(caplog.text) == 0


def test_validate_and_add_data_input_error_thrown_when_no_resource_downloaded(
    collection_dir, specification_dir, organisation_csv, mocker
):

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.request.headers = {"test": "test"}
    mock_response.headers = {"test": "test"}
    mock_response.content = ""
    mocker.patch(
        "requests.Session.get",
        return_value=mock_response,
    )
    collection_name = "conservation-area"
    no_error_input_data = {
        "organisation": "local-authority:SST",
        "documentation-url": "https://www.westoxon.gov.uk/planning-and-building/digital-planning-data/",
        "endpoint-url": "https://services5.arcgis.com/z8GJkxrWic0alJoM/arcgis/rest/services/WODC_Conservation_Areas_WGS/FeatureServer",
        "start-date": "",
        "pipelines": "conservation-area",
        "plugin": "",
        "licence": "ogl3",
    }

    tmp_input_path = create_input_csv(no_error_input_data)

    with pytest.raises(HTTPError) as error:
        validate_and_add_data_input(
            tmp_input_path,
            collection_name,
            collection_dir,
            specification_dir,
            organisation_csv,
        )

    assert "Failed to collect resource from URL" in str(error)
