import csv
import logging
import os
import tempfile
from unittest.mock import Mock
import pytest

from digital_land.commands import validate_and_add_data_input
from tests.acceptance.conftest import copy_latest_specification_files_to


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


def test_validate_and_add_data_input_missing_columns(
    collection_dir, specification_dir, organisation_csv, mock_request_get
):
    collection_name = "conservation-area"
    missing_column_input_data = {
        "organisation": "local-authority:SST",
        "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
        "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
        "start-date": "",
        "pipelines": "conservation-area",
        "plugin": "",
    }
    missing_column_fieldnames = [
        "organisation",
        "documentation-url",
        "endpoint-url",
        "start-date",
        "pipelines",
        "plugin",
    ]
    tmp_input_path = create_input_csv(
        missing_column_input_data, fieldnames=missing_column_fieldnames
    )

    with pytest.raises(Exception) as error:
        validate_and_add_data_input(
            tmp_input_path,
            collection_name,
            collection_dir,
            specification_dir,
            organisation_csv,
        )
    assert "required column (licence) not found in csv" in str(error)


@pytest.mark.parametrize(
    "input_data, error_message",
    [
        (
            {
                "organisation": "local-authority:SST",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
                "start-date": "",
                "pipelines": "conservation-area",
                "plugin": "",
                "licence": "",
            },
            "Licence is blank",
        ),
        (
            {
                "organisation": "local-authority:SST",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
                "start-date": "",
                "pipelines": "conservation-area",
                "plugin": "",
                "licence": "incorrect",
            },
            "'incorrect' is not a valid licence according to the specification",
        ),
        (
            {
                "organisation": "local-authority:SST",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "",
                "start-date": "",
                "pipelines": "conservation-area",
                "plugin": "",
                "licence": "ogl3",
            },
            "endpoint_url must be populated",
        ),
        (
            {
                "organisation": "local-authority:SST",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
                "start-date": "",
                "pipelines": "conservation-area",
                "plugin": "",
                "licence": "ogl3",
            },
            "endpoint_url must start with 'http://' or 'https://'",
        ),
        (
            {
                "organisation": "local-authority:SST",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "https://sstaffs",
                "start-date": "",
                "pipelines": "conservation-area",
                "plugin": "",
                "licence": "ogl3",
            },
            "The endpoint_url must have a valid domain with a top-level domain (e.g., '.gov.uk', '.com')",
        ),
        (
            {
                "organisation": "local-authority:SST",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
                "start-date": "01/01/2000",
                "pipelines": "conservation-area",
                "plugin": "",
                "licence": "ogl3",
            },
            "start-date 01/01/2000 must be format YYYY-MM-DD'",
        ),
        (
            {
                "organisation": "local-authority:SST",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
                "start-date": "9999-01-01",
                "pipelines": "conservation-area",
                "plugin": "",
                "licence": "ogl3",
            },
            "start-date 9999-01-01 cannot be in the future",
        ),
        (
            {
                "organisation": "",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
                "start-date": "",
                "pipelines": "conservation-area",
                "plugin": "",
                "licence": "ogl3",
            },
            "organisation must not be blank",
        ),
        (
            {
                "organisation": "???",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
                "start-date": "",
                "pipelines": "conservation-area",
                "plugin": "",
                "licence": "ogl3",
            },
            "'???' is not in our valid organisations",
        ),
        (
            {
                "organisation": "local-authority:SST",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
                "start-date": "",
                "pipelines": "conservation-area;invalid-pipeline",
                "plugin": "",
                "licence": "ogl3",
            },
            "'invalid-pipeline' is not a valid dataset in the specification",
        ),
        (
            {
                "organisation": "local-authority:SST",
                "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
                "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
                "start-date": "",
                "pipelines": "conservation-area;brownfield-land",
                "plugin": "",
                "licence": "ogl3",
            },
            "'brownfield-land' does not belong to provided collection conservation-area",
        ),
    ],
)
def test_validate_and_add_data(
    collection_dir,
    specification_dir,
    organisation_csv,
    mock_request_get,
    input_data,
    error_message,
):
    collection_name = "conservation-area"
    tmp_input_path = create_input_csv(input_data)
    with pytest.raises(ValueError) as error:
        validate_and_add_data_input(
            tmp_input_path,
            collection_name,
            collection_dir,
            specification_dir,
            organisation_csv,
        )
    assert error_message in str(error)


def test_validate_and_add_data_input_non_200(
    collection_dir, specification_dir, organisation_csv, capsys, mocker
):

    mock_response = Mock()
    mock_response.status_code = 404
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
        "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-are",
        "endpoint-url": "https://www.sstaffs.gov.uk/random_url",
        "start-date": "",
        "pipelines": "conservation-area",
        "plugin": "",
        "licence": "ogl3",
    }

    tmp_input_path = create_input_csv(no_error_input_data)

    validate_and_add_data_input(
        tmp_input_path,
        collection_name,
        collection_dir,
        specification_dir,
        organisation_csv,
    )

    assert "The status is not 200" in capsys.readouterr().out


def test_validate_and_add_data_input_duplicate_endpoint(
    collection_dir,
    specification_dir,
    organisation_csv,
    capsys,
    mock_request_get,
):
    endpoint_fieldnames = [
        "endpoint",
        "endpoint-url",
        "parameters",
        "plugin",
        "entry-date",
        "start-date",
        "end-date",
    ]
    endpoint_duplicate_row = {
        "endpoint": "ebeea8689113e04aa6e709520e0b437aa0f39e80952498dd00663d9cfabb2030",
        "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
        "parameters": "",
        "plugin": "",
        "entry-date": "",
        "start-date": "",
        "end-date": "",
    }
    with open(os.path.join(collection_dir, "endpoint.csv"), "a") as f:
        writer = csv.DictWriter(f, fieldnames=endpoint_fieldnames)
        writer.writeheader()
        writer.writerow(endpoint_duplicate_row)

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

    with pytest.raises(Exception):
        validate_and_add_data_input(
            tmp_input_path,
            collection_name,
            collection_dir,
            specification_dir,
            organisation_csv,
        )
    assert "endpoint already exists" in capsys.readouterr().out
