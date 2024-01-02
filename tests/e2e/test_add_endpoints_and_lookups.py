from datetime import datetime

import pytest
import os
import csv
import urllib

from pathlib import Path
from unittest.mock import Mock
from click.testing import CliRunner

from digital_land.commands import add_endpoints_and_lookups
from digital_land.collection import Collection
from digital_land.pipeline import Lookups
from digital_land.cli import add_endpoint_and_lookups_cmd

"""
A file to test adding an endpoint to a collection. This involves:
- adding endpoints,sources and lookups for a batch of endpoints from a csv
"""


@pytest.fixture
def endpoint_url_csv(tmp_path):
    """
    Writes the minimum data needed to add an endpoint for a single row and returns the file_path,
    """
    new_endpoints_csv_path = os.path.join(tmp_path, "new_endpoints.csv")
    row = {
        "endpoint-url": "https://www.example.com",  # mock this we're just going to assume that it returns data
        "documentation-url": "https://www.example.com",
        "organisation": "government-organisation:D1342",
        "start-date": "2023-08-10",
        "plugin": "",
        "pipelines": "ancient-woodland",
    }
    fieldnames = row.keys()
    with open(new_endpoints_csv_path, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    return new_endpoints_csv_path


@pytest.fixture
def wrong_endpoint_url_csv(tmp_path):
    """
    Writes the minimum data needed to add an endpoint for a single row and returns the file_path.
    This variant creates a csv with the wrong pipeline for the collection
    """
    new_endpoints_csv_path = os.path.join(tmp_path, "new_endpoints.csv")
    row = {
        "endpoint-url": "https://www.example.com",  # mock this we're just going to assume that it returns data
        "documentation-url": "https://www.example.com",
        "organisation": "government-organisation:D1342",
        "start-date": "2023-08-10",
        "plugin": "",
        "pipelines": "brownfield-land",
    }
    fieldnames = row.keys()
    with open(new_endpoints_csv_path, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    return new_endpoints_csv_path


@pytest.fixture
def mock_resource(tmp_path):
    data = {"reference": "1", "value": "test"}
    mock_csv_path = Path(tmp_path, "mock_csv.csv")
    with open(mock_csv_path, "w", encoding="utf-8") as f:
        dictwriter = csv.DictWriter(f, fieldnames=data.keys())
        dictwriter.writeheader()
        dictwriter.writerow(data)

    return mock_csv_path


@pytest.fixture
def collection_dir(tmp_path):
    collection_dir = os.path.join(tmp_path, "collection")
    os.makedirs(collection_dir, exist_ok=True)

    # create source
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


@pytest.fixture(scope="session")
def specification_dir(tmp_path_factory):
    specification_dir = tmp_path_factory.mktemp("specification")
    source_url = "https://raw.githubusercontent.com/digital-land/"
    specification_csvs = [
        "attribution.csv",
        "licence.csv",
        "typology.csv",
        "theme.csv",
        "collection.csv",
        "dataset.csv",
        "dataset-field.csv",
        "field.csv",
        "datatype.csv",
        "prefix.csv",
        # deprecated ..
        "pipeline.csv",
        "dataset-schema.csv",
        "schema.csv",
        "schema-field.csv",
    ]
    for specification_csv in specification_csvs:
        urllib.request.urlretrieve(
            f"{source_url}/specification/main/specification/{specification_csv}",
            os.path.join(specification_dir, specification_csv),
        )

    return specification_dir


@pytest.fixture
def organisation_csv(tmp_path):
    organisation_path = os.path.join(tmp_path, "organisation.csv")
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv",
        organisation_path,
    )
    return organisation_path


@pytest.fixture
def pipeline_dir(tmp_path):
    pipeline_dir = os.path.join(tmp_path, "pipeline")
    os.makedirs(pipeline_dir, exist_ok=True)

    # create lookups
    row = {
        "prefix": "ancient-woodland",
        "resource": "",
        "organisation": "local-authority-eng:ABC",
        "reference": "ABC_0001",
        "entity": "1234567",
    }
    fieldnames = row.keys()

    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    return pipeline_dir


def test_command_add_endpoints_and_lookups_success_lookups_required(
    endpoint_url_csv,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    mocker,
    mock_resource,
):
    """
    We need to be able to automatically add a set of endpoints from a file of endpoint-urls
    this includes adding sources for each one and identifying additional lookups.
    """

    with open(mock_resource, "r", encoding="utf-8") as f:
        csv_content = f.read().encode("utf-8")

    collection_name = "testing"
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.request.headers = {"test": "test"}
    mock_response.headers = {"test": "test"}
    mock_response.content = csv_content
    mocker.patch(
        "requests.Session.get",
        return_value=mock_response,
    )

    add_endpoints_and_lookups(
        csv_file_path=endpoint_url_csv,
        collection_name=collection_name,
        collection_dir=Path(collection_dir),
        specification_dir=specification_dir,
        organisation_path=organisation_path,
        pipeline_dir=pipeline_dir,
    )

    # test endpoints and sources have been added
    collection = Collection(name=collection_name, directory=collection_dir)
    collection.load()

    assert len(collection.source.entries) > 0
    assert len(collection.endpoint.entries) > 0

    # check logs
    assert len(collection.log.entries) > 0

    # test lookups have been added correctly, including
    lookups = Lookups(pipeline_dir)
    lookups.load_csv()

    assert len(lookups.entries) > 0

    for entry in lookups.entries:
        for expected_key in ["organisation", "prefix", "entity", "reference"]:
            assert entry.get(expected_key, None) is not None

    expected_entry_date = datetime.now().strftime("%Y-%m-%d")
    for source in collection.source.entries:
        assert source.get("entry-date", None) is not None
        assert source.get("entry-date", None) == expected_entry_date

    for endpoint in collection.endpoint.entries:
        assert endpoint.get("entry-date", None) is not None
        assert endpoint.get("entry-date", None) == expected_entry_date


def test_cli_add_endpoints_and_lookups_cmd_success_return_code(
    endpoint_url_csv,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    mocker,
    mock_resource,
):
    """
    same test as above but incorporate running the command
    the check that cli to command function works appropriately
    """

    with open(mock_resource, "r", encoding="utf-8") as f:
        csv_content = f.read().encode("utf-8")

    collection_name = "testing"
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.request.headers = {"test": "test"}
    mock_response.headers = {"test": "test"}
    mock_response.content = csv_content
    mocker.patch(
        "requests.Session.get",
        return_value=mock_response,
    )

    runner = CliRunner()
    result = runner.invoke(
        add_endpoint_and_lookups_cmd,
        [
            endpoint_url_csv,
            collection_name,
            # these will be optional to the user but included here to point at files stored elsewhere
            "--collection-dir",
            collection_dir,
            "--pipeline-dir",
            pipeline_dir,
            "--specification-dir",
            specification_dir,
            "--organisation-path",
            organisation_path,
        ],
    )

    assert result.exit_code == 0, f"{result.stdout}"


# adding endpoint requirements
# start date should be provided
# adding source requirements

# passing both Path and strs for directory structures
