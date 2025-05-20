import csv
from datetime import datetime
import os
from pathlib import Path
import shutil
import tempfile
from unittest import mock
from unittest.mock import Mock, patch
from click.testing import CliRunner
import pandas as pd
import pytest

from digital_land.cli import cli
from tests.acceptance.conftest import copy_latest_specification_files_to


@pytest.fixture(scope="module")
def specification_dir(tmp_path_factory):
    specification_dir = tmp_path_factory.mktemp("specification")
    copy_latest_specification_files_to(specification_dir)
    return specification_dir


@pytest.fixture
def pipeline_dir(tmp_path_factory):
    pipeline_dir = tmp_path_factory.mktemp("pipeline")

    collection_name = "ancient-woodland"
    # create lookups
    row = {
        "prefix": collection_name,
        "resource": "",
        "entry-number": "",
        "organisation": "local-authority:SST",
        "reference": "reference",
        "entity": 44000001,
    }

    fieldnames = row.keys()

    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    return pipeline_dir


@pytest.fixture
def cache_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("cache")


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
    data = "reference,documentation-url\n1,url"
    csv_content = data.encode("utf-8")
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.request.headers = {"test": "test"}
    mock_response.headers = {"test": "test"}
    mock_response.content = csv_content
    mocker.patch(
        "requests.Session.get",
        return_value=mock_response,
    )


@pytest.fixture
def mock_request_get_no_reference(mocker):
    data = "reference,documentation-url\n,url"
    csv_content = data.encode("utf-8")
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.request.headers = {"test": "test"}
    mock_response.headers = {"test": "test"}
    mock_response.content = csv_content
    mocker.patch(
        "requests.Session.get",
        return_value=mock_response,
    )


@pytest.fixture
def mock_download_dataset():
    original_dataset_path = Path("tests/data/dataset/central-activities-zone.sqlite3")
    updated_dataset_path = tempfile.NamedTemporaryFile(suffix=".sqlite3").name
    # copy so we can update a version to compare to original
    shutil.copy(original_dataset_path, updated_dataset_path)
    with patch(
        "digital_land.commands.download_dataset",
        return_value=Path(updated_dataset_path),
    ) as mock:
        yield mock


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
    cache_dir,
    organisation_csv,
    mock_request_get,
    mock_download_dataset,
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
            "--pipeline-dir",
            str(pipeline_dir),
            "--organisation-path",
            str(organisation_csv),
            "--cache-dir",
            str(cache_dir),
        ],
    )
    if result.exit_code != 0:
        # Print the command output if the test fails, gives more detail on what's gone wrong
        print("Command failed with exit code:", result.exit_code)
        print("Command output:")
        print(result.output)
        print("Command error output:")
        print(result.exception)

    assert result.exit_code == 0
    # Check summaries are in stdout
    assert "Endpoint and source details" in result.stdout
    assert "Run pipeline" in result.stdout
    assert "Column Field Summary" in result.stdout
    assert "Issue Summary" in result.stdout
    assert "Entity Summary" in result.stdout

    # Check entity assigning has worked
    assert "Total number of new entities: 1" in result.stdout
    assert "No new entities in resource" in result.stdout


def test_cli_add_data_incorrect_input_data(
    collection_dir,
    specification_dir,
    pipeline_dir,
    organisation_csv,
    mock_request_get,
    mock_download_dataset,
    cache_dir,
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
            "--pipeline-dir",
            str(pipeline_dir),
            "--organisation-path",
            str(organisation_csv),
            "--cache-dir",
            str(cache_dir),
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
    mock_download_dataset,
    monkeypatch,
    cache_dir,
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
            "--pipeline-dir",
            str(pipeline_dir),
            "--organisation-path",
            str(organisation_csv),
            "--cache-dir",
            str(cache_dir),
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
            "--pipeline-dir",
            str(pipeline_dir),
            "--organisation-path",
            str(organisation_csv),
            "--cache-dir",
            str(cache_dir),
        ],
    )
    assert result.exit_code == 0
    # Check summaries are in stdout
    assert "Endpoint and source details" in result.stdout
    assert "Run pipeline" in result.stdout
    assert "Column Field Summary" in result.stdout
    assert "Issue Summary" in result.stdout
    assert "Entity Summary" in result.stdout

    # Check entity assigning has worked
    assert "Total number of new entities: 1" in result.stdout
    assert "No new entities in resource" in result.stdout


def test_cli_add_data_pipeline_fail(
    collection_dir,
    specification_dir,
    pipeline_dir,
    cache_dir,
    organisation_csv,
    mock_request_get,
    mock_download_dataset,
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
    with mock.patch("digital_land.commands.pipeline_run") as pipeline_mock:
        pipeline_mock.side_effect = Exception("Exception while running pipeline")
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
                "--pipeline-dir",
                str(pipeline_dir),
                "--organisation-path",
                str(organisation_csv),
                "--cache-dir",
                str(cache_dir),
            ],
        )

    assert result.exit_code == 1
    assert "Pipeline failed to process resource with the following error" in str(
        result.exception
    )
    assert "Exception while running pipeline" in str(result.exception)


def test_cli_add_data_remaining_unassigned_entities(
    collection_dir,
    specification_dir,
    pipeline_dir,
    cache_dir,
    organisation_csv,
    mock_request_get_no_reference,
    mock_download_dataset,
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
            "--pipeline-dir",
            str(pipeline_dir),
            "--organisation-path",
            str(organisation_csv),
            "--cache-dir",
            str(cache_dir),
        ],
    )

    assert result.exit_code == 1
    assert "Unknown entities remain in resource" in str(result.exception)


def test_cli_add_data_old_endpoints_retired(
    collection_dir,
    specification_dir,
    pipeline_dir,
    cache_dir,
    organisation_csv,
    mock_request_get,
    mock_download_dataset,
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

    # Now add old endpoints/source to be retired
    source = {
        "attribution": "",
        "collection": "conservation-area",
        "documentation-url": "",
        "endpoint": "endpoint",
        "licence": "",
        "organisation": "local-authority:SST",
        "pipelines": "conservation-area",
        "entry-date": "now!",
        "start-date": "",
        "end-date": "",
    }
    with open(os.path.join(collection_dir, "source.csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=source.keys())
        writer.writeheader()
        writer.writerow(source)

    endpoint = {
        "endpoint": "endpoint",
        "endpoint-url": "endpoint-url",
        "parameters": "",
        "plugin": "",
        "entry-date": "now!",
        "start-date": "",
        "end-date": "",
    }

    with open(os.path.join(collection_dir, "endpoint.csv"), "w") as f:
        writer = csv.DictWriter(f, fieldnames=endpoint.keys())
        writer.writeheader()
        writer.writerow(endpoint)

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
            "--pipeline-dir",
            str(pipeline_dir),
            "--organisation-path",
            str(organisation_csv),
            "--cache-dir",
            str(cache_dir),
        ],
    )
    if result.exit_code != 0:
        # Print the command output if the test fails, gives more detail on what's gone wrong
        print("Command failed with exit code:", result.exit_code)
        print("Command output:")
        print(result.output)
        print("Command error output:")
        print(result.exception)

    assert result.exit_code == 0
    # Check old endpoint summary in stdout
    print(result.stdout)
    assert "Existing endpoints found for this provision:" in result.stdout
    assert "now!, endpoint-url" in result.stdout

    # check that the endpoint and source have been retired
    endpoint_df = pd.read_csv(os.path.join(collection_dir, "endpoint.csv"))
    assert endpoint_df["end-date"].values[0] == datetime.utcnow().isoformat()[:10]

    source_df = pd.read_csv(os.path.join(collection_dir, "source.csv"))
    assert source_df["end-date"].values[0] == datetime.utcnow().isoformat()[:10]


# Add acceptance test
# @patch("digital_land.commands.API.download_dataset", return_value=Path("tests/data/dataset/central-activities-zone.sqlite3"))
def test_cli_add_data_update_dataset(
    collection_dir,
    specification_dir,
    pipeline_dir,
    cache_dir,
    organisation_csv,
    mock_request_get,
    mock_download_dataset,
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
            "--pipeline-dir",
            str(pipeline_dir),
            "--organisation-path",
            str(organisation_csv),
            "--cache-dir",
            str(cache_dir),
        ],
    )
    if result.exit_code != 0:
        # Print the command output if the test fails, gives more detail on what's gone wrong
        print("Command failed with exit code:", result.exit_code)
        print("Command output:")
        print(result.output)
        print("Command error output:")
        print(result.exception)

    assert result.exit_code == 0
    print("result std out", result.stdout)
    assert "Entity: 44000000, Fields changed:" in result.stdout
