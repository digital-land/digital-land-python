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
from digital_land.specification import Specification

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
        "licence": "cc0",
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
def mock_resource_identical_reference(tmp_path):
    data = {"reference": "ABC_0001", "value": "test"}
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
def pipeline_dir(tmp_path, specification_dir):
    pipeline_dir = os.path.join(tmp_path, "pipeline")
    os.makedirs(pipeline_dir, exist_ok=True)

    collection_name = "ancient-woodland"
    specification = Specification(specification_dir)
    entity_range_min = specification.get_dataset_entity_min(collection_name)
    # create lookups
    row = {
        "prefix": collection_name,
        "resource": "",
        "entry-number": "",
        "organisation": "local-authority-eng:ABC",
        "reference": "ABC_0001",
        "entity": entity_range_min,
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

    collection_name = "ancient-woodland"
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

    specification = Specification(specification_dir)
    entity_range_min = specification.get_dataset_entity_min(collection_name)

    for entry in lookups.entries:
        for expected_key in ["organisation", "prefix", "entity", "reference"]:
            assert entry.get(expected_key, None) is not None
            # Check if the entity range in the lookups file is within the specification
        assert entry.get("entity") == entity_range_min
        entity_range_min = str(int(entity_range_min) + 1)

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

    collection_name = "ancient-woodland"
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


def test_cli_add_endpoints_and_lookups_cmd_error_for_extra_columns(
    endpoint_url_csv,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    mocker,
    mock_resource,
):
    """
    generate a lookup.csv file with an extra column. This should
    cause an error, rather than just dropping the column.
    """

    collection_name = "ancient-woodland"

    # Re-generate lookups with the extra field
    row = {
        "prefix": collection_name,
        "resource": "",
        "entry-number": 1,
        "organisation": "local-authority-eng:ABC",
        "reference": "ABC_0001",
        "entity": 12345,
        "cheese": "Wensleydale",
    }
    with open(os.path.join(pipeline_dir, "lookup.csv"), "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=row.keys())
        dictwriter.writeheader()
        dictwriter.writerow(row)

    with open(mock_resource, "r", encoding="utf-8") as f:
        csv_content = f.read().encode("utf-8")

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

    assert result.exit_code != 0, f"{result.stdout}"


def test_command_add_endpoints_and_lookups_flags_duplicate_endpoint(
    endpoint_url_csv,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    mocker,
    mock_resource,
):
    """
    Checks existing endpoints for every new endpoint added. If the new endpoint is a duplicate
    then raises this is an issue and prevents adding it to the system
    """

    with open(mock_resource, "r", encoding="utf-8") as f:
        csv_content = f.read().encode("utf-8")

    collection_name = "ancient-woodland"
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

    collection = Collection(name=collection_name, directory=collection_dir)
    collection.load()

    runner = CliRunner()
    result = runner.invoke(
        add_endpoint_and_lookups_cmd,
        [
            endpoint_url_csv,
            collection_name,
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

    cli_output = f"{result.stdout}"
    assert ">>> INFO: endpoint already exists" in cli_output
    assert ">>> Endpoint URL https://www.example.com" in cli_output

    assert len(collection.source.entries) == 1
    assert len(collection.endpoint.entries) == 1


@pytest.fixture
def invalid_license_endpoint_csv(tmp_path):
    """
    Creates a CSV with an endpoint that has a non-existent license, simulating a scenario where the license doesn't match the specification.
    """
    new_endpoints_csv_path = os.path.join(tmp_path, "invalid_license_endpoints.csv")
    row = {
        "endpoint-url": "https://www.example.com",
        "documentation-url": "https://www.example.com",
        "organisation": "government-organisation:D1342",
        "start-date": "2023-08-10",
        "licence": "non-existent-license",
        "plugin": "",
        "pipelines": "ancient-woodland",
    }
    fieldnames = row.keys()
    with open(new_endpoints_csv_path, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    return new_endpoints_csv_path


def test_add_endpoints_with_invalid_license_raises_value_error(
    invalid_license_endpoint_csv,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    mocker,
):
    """
    Test to ensure that providing an endpoint with an invalid license raises a ValueError.
    """
    with pytest.raises(ValueError) as excinfo:
        add_endpoints_and_lookups(
            csv_file_path=invalid_license_endpoint_csv,
            collection_name="ancient-woodland",
            collection_dir=Path(collection_dir),
            pipeline_dir=pipeline_dir,
            specification_dir=specification_dir,
            organisation_path=organisation_path,
        )
    assert "Licence 'non-existent-license' is not a valid licence" in str(excinfo.value)


def test_command_add_endpoints_and_lookups_considers_organisation(
    endpoint_url_csv,
    collection_dir,
    pipeline_dir,
    specification_dir,
    organisation_path,
    mocker,
    mock_resource_identical_reference,
    capfd,
):
    """
    When assigning new lookups we need to make sure new lookups are assigned even when a lookup with the same reference already exists for an organisation.
    """

    with open(mock_resource_identical_reference, "r", encoding="utf-8") as f:
        csv_content = f.read().encode("utf-8")

    collection_name = "ancient-woodland"
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
    out = capfd.readouterr()

    # assert to verify new lookup with the same reference is added
    assert len(lookups.entries) == 2
    assert (
        "ancient-woodland , government-organisation:D1342 , ABC_0001 , 110000001"
        in out[0]
    )


# adding endpoint requirements
# start date should be provided
# adding source requirements

# passing both Path and strs for directory structures
