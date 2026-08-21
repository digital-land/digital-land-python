"""
A set of tests to mimick a user (computational or otherwise) running tests against
a sqlite dataset. There are quite a few things to set up and this specifically
"""

import pytest
import spatialite
import csv
import duckdb
import logging
import json
import pandas as pd

from click.testing import CliRunner

from digital_land.cli import expectations_run_dataset_checkpoint
from digital_land.configuration.main import Config
from digital_land.specification import Specification


@pytest.fixture
def dataset_path(tmp_path):
    dataset_path = tmp_path / "test.sqlite3"

    # schemas are locked incase the spec changes
    # in the future  we may want to generalise this
    create_entity_table_sql = """
        CREATE TABLE entity (
            dataset TEXT,
            end_date TEXT,
            entity INTEGER PRIMARY KEY,
            entry_date TEXT,
            geojson JSON,
            geometry TEXT,
            json JSON,
            name TEXT,
            organisation_entity TEXT,
            point TEXT,
            prefix TEXT,
            reference TEXT,
            start_date TEXT,
            typology TEXT
        );
    """

    create_old_entity_table_sql = """
        CREATE TABLE old_entity (
            old_entity INTEGER PRIMARY KEY,
            entity INTEGER
        );
    """
    test_entity_data = pd.DataFrame.from_dict(
        {
            "entity": [1],
            "name": ["test1"],
            "organisation_entity": [122],
            "geometry": [
                "MULTIPOLYGON(((-0.4914554581046105 53.80708847427775,-0.5012039467692374 53.773842823566696,-0.4584064520895481 53.783669118729875,-0.4914554581046105 53.80708847427775)))"  # noqa E501
            ],
            "point": ["POINT(-0.4850078825017034 53.786407721600625)"],
        }
    )
    test_old_entity_data = pd.DataFrame.from_dict({"old_entity": [100], "entity": [10]})
    # define rows to load in these have been prebuilt to fit into a ccertain area
    # you can view the WKTs at .... they are designed to fit in the lpa boundary  below

    with spatialite.connect(dataset_path) as con:
        con.execute(create_entity_table_sql)
        con.execute(create_old_entity_table_sql)
        test_entity_data.to_sql("entity", con, if_exists="append", index=False)
        test_old_entity_data.to_sql("old_entity", con, if_exists="append", index=False)

    return dataset_path


@pytest.fixture
def organisation_path(tmp_path):
    """
    build an organisations dataset to use
    """
    orgs_path = tmp_path / "organisation.csv"
    orgs = [
        {
            "entity": 122,
            "name": "test",
            "prefix": "local-authority",
            "reference": "test",
            "dataset": "local-authority",
            "organisation": "local-authority:test",
        },
        {
            "entity": 102,
            "name": "test_2",
            "prefix": "local-authority",
            "reference": "test_2",
            "dataset": "local-authority",
            "organisation": "local-authority:test_2",
        },
    ]

    with open(orgs_path, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=orgs[0].keys())
        dictwriter.writeheader()
        dictwriter.writerows(orgs)

    return orgs_path


@pytest.fixture
def config_path(tmp_path, specification_dir):
    """create a configuration to use"""
    config_path = tmp_path / "config.sqllite3"
    rules = [
        {
            "datasets": "test",
            "organisations": "local-authority:test",
            "name": "test rule for {{ organisation.name }}",
            "operation": "count_lpa_boundary",
            "parameters": '{"expected":1,"lpa":"{{ organisation.local_planning_authority }}","organisation_entity":"{{ organisation.entity }}"}',
            "responsibility": "internal",
            "severity": "notice",
        }
    ]
    # write rows
    expect_path = tmp_path / "expect.csv"
    # Writing data to CSV
    with open(expect_path, mode="w", newline="") as file:
        # Define the fieldnames (column headers) based on dictionary keys
        writer = csv.DictWriter(file, fieldnames=rules[0].keys())
        writer.writeheader()  # Write the header row
        writer.writerows(rules)  # Write each dictionary as a row

    spec = Specification(specification_dir)
    config = Config(path=config_path, specification=spec)
    config.create()
    config.load({"expect": str(tmp_path)})
    return config_path


@pytest.fixture
def name_check_dataset_path(tmp_path):
    """a dataset with one bare code name and one descriptive name"""
    dataset_path = tmp_path / "name_check.sqlite3"
    create_entity_table_sql = """
        CREATE TABLE entity (
            dataset TEXT,
            end_date TEXT,
            entity INTEGER PRIMARY KEY,
            entry_date TEXT,
            geojson JSON,
            geometry TEXT,
            json JSON,
            name TEXT,
            organisation_entity TEXT,
            point TEXT,
            prefix TEXT,
            reference TEXT,
            start_date TEXT,
            typology TEXT
        );
    """
    with spatialite.connect(dataset_path) as con:
        con.execute(create_entity_table_sql)
        con.executemany(
            "INSERT INTO entity (entity, reference, name, organisation_entity)"
            " VALUES (?, ?, ?, ?)",
            [
                (1, "CA-59", "59", "122"),
                (2, "CA-WYM", "Wymondham Conservation Area", "122"),
            ],
        )

    return dataset_path


@pytest.fixture
def name_check_config_path(tmp_path, specification_dir):
    """a configuration enabling name_is_a_code_check, with no organisations and empty
    parameters, matching how the rule will be written in config"""
    config_path = tmp_path / "name_check_config.sqlite3"
    rules = [
        {
            "datasets": "test",
            "organisations": "",
            "name": "Check no entities have a name which is only a code",
            "operation": "name_is_a_code_check",
            "parameters": "{}",
            "responsibility": "external",
            "severity": "warning",
        }
    ]
    expect_path = tmp_path / "expect.csv"
    with open(expect_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=rules[0].keys())
        writer.writeheader()
        writer.writerows(rules)

    spec = Specification(specification_dir)
    config = Config(path=config_path, specification=spec)
    config.create()
    config.load({"expect": str(tmp_path)})
    return config_path


@pytest.fixture
def placeholder_check_dataset_path(tmp_path):
    """a dataset with one placeholder name and one real name, both linked to a
    listed-building record"""
    dataset_path = tmp_path / "placeholder_check.sqlite3"
    create_entity_table_sql = """
        CREATE TABLE entity (
            dataset TEXT,
            end_date TEXT,
            entity INTEGER PRIMARY KEY,
            entry_date TEXT,
            geojson JSON,
            geometry TEXT,
            json JSON,
            name TEXT,
            organisation_entity TEXT,
            point TEXT,
            prefix TEXT,
            reference TEXT,
            start_date TEXT,
            typology TEXT
        );
    """
    with spatialite.connect(dataset_path) as con:
        con.execute(create_entity_table_sql)
        con.executemany(
            "INSERT INTO entity (entity, reference, name, organisation_entity, json)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (
                    1,
                    "LBO-A",
                    "No name for this Entry",
                    "122",
                    json.dumps({"listed-building": "1234567"}),
                ),
                (
                    2,
                    "LBO-C",
                    "NORTH LODGE",
                    "122",
                    json.dumps({"listed-building": "2222222"}),
                ),
            ],
        )

    return dataset_path


@pytest.fixture
def listed_building_artifact_path(tmp_path):
    """stands in for the listed-building sqlite fetched from S3 during assemble"""
    artifact_path = tmp_path / "listed-building.sqlite3"
    with spatialite.connect(artifact_path) as con:
        con.execute(
            """
            CREATE TABLE entity (
                entity INTEGER PRIMARY KEY,
                name TEXT,
                reference TEXT
            );
        """
        )
        con.executemany(
            "INSERT INTO entity (entity, name, reference) VALUES (?, ?, ?)",
            [
                (900001, "33, 37 AND 39, BAYFORD GREEN", "1234567"),
                (900002, "NORTH LODGE", "2222222"),
            ],
        )

    return artifact_path


@pytest.fixture
def placeholder_check_config_path(
    tmp_path, specification_dir, listed_building_artifact_path
):
    """a configuration enabling name_is_a_placeholder_check, carrying the artifact path
    in parameters exactly as the rule will be written in config"""
    config_path = tmp_path / "placeholder_check_config.sqlite3"
    rules = [
        {
            "datasets": "test",
            "organisations": "",
            "name": "Check no entities have a placeholder name",
            "operation": "name_is_a_placeholder_check",
            "parameters": json.dumps(
                {"listed_building_path": str(listed_building_artifact_path)}
            ),
            "responsibility": "external",
            "severity": "error",
        }
    ]
    expect_path = tmp_path / "expect.csv"
    with open(expect_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=rules[0].keys())
        writer.writeheader()
        writer.writerows(rules)

    spec = Specification(specification_dir)
    config = Config(path=config_path, specification=spec)
    config.create()
    config.load({"expect": str(tmp_path)})
    return config_path


def test_run_some_expectations(
    tmp_path, organisation_path, dataset_path, config_path, specification_dir, mocker
):

    # set up inputs
    dataset = "test"

    # set up mockign of API response, we don't want to be dependannt on the data returned
    # this coulld be lifted in the future if we just want to ensure it rurns
    # even if the test fails
    lpa_geometry = "MULTIPOLYGON(((-0.49901924973862233 53.81622315189787,-0.5177418530633007 53.76114469621959,-0.4268378912177833 53.78454002743749,-0.49901924973862233 53.81622315189787)))"  # noqa E501
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "geometry": lpa_geometry,
    }

    # Mock the `requests.Session.get` method
    mocker.patch("requests.get", return_value=mock_response)
    # set up cli runner
    runner = CliRunner()

    result = runner.invoke(
        expectations_run_dataset_checkpoint,
        [
            "--dataset",
            dataset,
            "--file-path",
            str(dataset_path),
            "--log-dir",
            str(tmp_path / "log"),
            "--configuration-path",
            str(config_path),
            "--organisation-path",
            str(organisation_path),
            "--specification-dir",
            str(specification_dir),
        ],
        catch_exceptions=False,
    )

    # Check that the command exits with status code 0 (success)
    if result.exit_code != 0:
        # Print the command output if the test fails
        print("Command failed with exit code:", result.exit_code)
        print("Command output:")
        print(result.output)
        print("Command error output:")
        print(result.exception)

    assert result.exit_code == 0, "error returned when running expectations"
    assert (
        tmp_path / f"log/expectation/dataset={dataset}/{dataset}.parquet"
    ).exists(), "no logs created"

    # logs are there
    conn = duckdb.connect()

    # Query the partitioned directory of Parquet files
    query = f"""
        SELECT *
        FROM read_parquet('{str(tmp_path / 'log/expectation' / '**/*.parquet')}');
    """

    # Execute the query
    cursor = conn.execute(query)

    # Get column names from the cursor
    columns = [desc[0] for desc in cursor.description]

    # Fetch all results as a list of dictionaries
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    logging.debug(f"The expectation log produced:\n{json.dumps(results, indent=4)}")
    # we need to check that a log was made when running the above as we inputted a test
    assert len(results) > 0, "check that some logs were created"
    # we need to check for some specific user required fields in the output
    # these have been confirmed as required outputs for any test
    required_fields = [
        "passed",
        "name",
        "details",
        "parameters",
        "organisation",
        "dataset",
        "operation",
    ]
    for field in required_fields:
        assert field in results[0].keys(), f"field : {field} not found in log"
    assert (
        len([result for result in results if result["passed"] == "True"]) == 1
    ), "expected 1 passed expectation"


def test_run_name_is_a_code_expectation(
    tmp_path,
    organisation_path,
    name_check_dataset_path,
    name_check_config_path,
    specification_dir,
):
    dataset = "test"
    runner = CliRunner()

    result = runner.invoke(
        expectations_run_dataset_checkpoint,
        [
            "--dataset",
            dataset,
            "--file-path",
            str(name_check_dataset_path),
            "--log-dir",
            str(tmp_path / "log"),
            "--configuration-path",
            str(name_check_config_path),
            "--organisation-path",
            str(organisation_path),
            "--specification-dir",
            str(specification_dir),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output

    log_path = tmp_path / f"log/expectation/dataset={dataset}/{dataset}.parquet"
    assert log_path.exists(), "no logs created"

    conn = duckdb.connect()
    cursor = conn.execute(f"SELECT * FROM read_parquet('{str(log_path)}')")
    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    assert len(results) == 1, "expected one expectation to have run"
    log = results[0]
    assert log["operation"] == "name_is_a_code_check"
    assert log["passed"] == "False", "the bare code name should have failed the check"
    assert log["severity"] == "warning"
    assert log["responsibility"] == "external"
    assert (
        log["organisation"] == ""
    ), "a rule with no organisations logs a blank organisation"

    details = json.loads(log["details"])
    assert details["actual"] == 1
    assert [failure["name"] for failure in details["failures"]] == ["59"]
    assert details["failures"][0]["organisation_entity"] == "122"


def test_run_name_is_a_placeholder_expectation(
    tmp_path,
    organisation_path,
    placeholder_check_dataset_path,
    placeholder_check_config_path,
    specification_dir,
):
    dataset = "test"
    runner = CliRunner()

    result = runner.invoke(
        expectations_run_dataset_checkpoint,
        [
            "--dataset",
            dataset,
            "--file-path",
            str(placeholder_check_dataset_path),
            "--log-dir",
            str(tmp_path / "log"),
            "--configuration-path",
            str(placeholder_check_config_path),
            "--organisation-path",
            str(organisation_path),
            "--specification-dir",
            str(specification_dir),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output

    log_path = tmp_path / f"log/expectation/dataset={dataset}/{dataset}.parquet"
    assert log_path.exists(), "no logs created"

    conn = duckdb.connect()
    cursor = conn.execute(f"SELECT * FROM read_parquet('{str(log_path)}')")
    columns = [desc[0] for desc in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    assert len(results) == 1, "expected one expectation to have run"
    log = results[0]
    assert log["operation"] == "name_is_a_placeholder_check"
    assert log["passed"] == "False", "the placeholder name should have failed the check"
    assert log["severity"] == "error"
    assert log["responsibility"] == "external"
    assert (
        log["organisation"] == ""
    ), "a rule with no organisations logs a blank organisation"

    details = json.loads(log["details"])
    assert details["actual"] == 1
    assert [failure["name"] for failure in details["failures"]] == [
        "No name for this Entry"
    ]
    assert (
        details["failures"][0]["listed_building_name"] == "33, 37 AND 39, BAYFORD GREEN"
    ), "the real name must reach details so the bridge can quote it back to the LPA"
