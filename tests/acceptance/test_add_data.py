import csv
import os
import tempfile
from unittest.mock import Mock
from click.testing import CliRunner
import pytest

from digital_land.cli import cli
from tests.acceptance.conftest import copy_latest_specification_files_to


@pytest.fixture(scope="module")
def specification_dir(tmp_path_factory):
    specification_dir = tmp_path_factory.mktemp("specification")
    copy_latest_specification_files_to(specification_dir)
    return specification_dir


@pytest.fixture(scope="function")
def pipeline_dir(tmp_path_factory):
    pipeline_dir = tmp_path_factory.mktemp("pipeline")
    return pipeline_dir


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


def test_cli_add_data(
    collection_dir,
    specification_dir,
    pipeline_dir,
    organisation_csv,
    mock_request_get,
    monkeypatch,
):
    no_error_input_data = {
        "organisation": "local-authority:SST",
        "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
        "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
        "start-date": "",
        "pipelines": "conservation-area",
        "plugin": "",
        "licence": "ogl3",
    }
    csv_path = create_input_csv(no_error_input_data)

    # Mock in user input
    monkeypatch.setattr("builtins.input", lambda _: "yes")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "add-data",
            csv_path,
            "conservation-area",
            "--collection-dir",
            str(collection_dir),
            "--specification-dir",
            str(specification_dir),
            "--organisation-path",
            str(organisation_csv),
        ],
    )

    assert result.exit_code == 0


def test_cli_add_data_incorrect_input_data(
    collection_dir,
    specification_dir,
    pipeline_dir,
    organisation_csv,
    mock_request_get,
):
    incorrect_input_data = {
        "organisation": "",
        "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
        "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
        "start-date": "",
        "pipelines": "conservation-area",
        "plugin": "",
        "licence": "ogl3",
    }
    csv_path = create_input_csv(incorrect_input_data)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "add-data",
            csv_path,
            "conservation-area",
            "--collection-dir",
            str(collection_dir),
            "--specification-dir",
            str(specification_dir),
            "--organisation-path",
            str(organisation_csv),
        ],
    )
    assert result.exit_code == 1
    assert "organisation must not be blank" in str(result.exception)


# This test exists as there is potential for the collection.load() to fail when
# there are leftover log files from a previous run
def test_cli_add_data_consecutive_runs(
    collection_dir,
    specification_dir,
    pipeline_dir,
    organisation_csv,
    mock_request_get,
    monkeypatch,
):
    no_error_input_data = {
        "organisation": "local-authority:SST",
        "documentation-url": "https://www.sstaffs.gov.uk/planning/conservation-and-heritage/south-staffordshires-conservation-areas",
        "endpoint-url": "https://www.sstaffs.gov.uk/sites/default/files/2024-11/South Staffs Conservation Area document dataset_1.csv",
        "start-date": "",
        "pipelines": "conservation-area",
        "plugin": "",
        "licence": "ogl3",
    }
    csv_path = create_input_csv(no_error_input_data)

    # Mock in user input
    monkeypatch.setattr("builtins.input", lambda _: "no")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "add-data",
            csv_path,
            "conservation-area",
            "--collection-dir",
            str(collection_dir),
            "--specification-dir",
            str(specification_dir),
            "--organisation-path",
            str(organisation_csv),
        ],
    )
    assert result.exit_code == 0

    monkeypatch.setattr("builtins.input", lambda _: "yes")
    # Now run a second time
    result = runner.invoke(
        cli,
        [
            "add-data",
            csv_path,
            "conservation-area",
            "--collection-dir",
            str(collection_dir),
            "--specification-dir",
            str(specification_dir),
            "--organisation-path",
            str(organisation_csv),
        ],
    )
    assert result.exit_code == 0
