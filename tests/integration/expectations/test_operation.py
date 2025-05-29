import spatialite
import sqlite3
import pytest
import pandas as pd

from digital_land.expectations.operation import (
    check_columns,
    count_lpa_boundary,
    count_deleted_entities,
    duplicate_geometry_check,
)


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
    with spatialite.connect(dataset_path) as con:
        con.execute(create_entity_table_sql)
        con.execute(create_old_entity_table_sql)

    return dataset_path


# define lpa_geometry instead of lpa in the params as response is mocked
@pytest.mark.parametrize(
    "expected,geometric_relation,comparison_rule",
    [
        (1, "within", "equals_to"),
        (1, "intersects", "equals_to"),
        (0, "not_intersects", "equals_to"),
        (1, "not_intersects", "less_than"),
        (1, "within", "less_than_or_equal_to"),
        (0, "within", "greater_than"),
        (1, "within", "greater_than_or_equal_to"),
        (1, "centroid_within", "equals_to"),
    ],
)
def test_count_lpa_boundary_passes(
    expected, geometric_relation, comparison_rule, dataset_path, mocker
):
    # define parameters constant parameters that aren't parametised
    organisation_entity = 122
    lpa = "test"
    # load data into sqlite
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

    # mock api
    # returned geometry value
    lpa_geometry = "MULTIPOLYGON(((-0.49901924973862233 53.81622315189787,-0.5177418530633007 53.76114469621959,-0.4268378912177833 53.78454002743749,-0.49901924973862233 53.81622315189787)))"  # noqa E501
    mock_response = mocker.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "geometry": lpa_geometry,
    }

    # Mock the `requests.Session.get` method
    mocker.patch("requests.get", return_value=mock_response)

    # Initialize the APIClient and UsersAPI
    with spatialite.connect(dataset_path) as conn:
        # load data
        test_entity_data.to_sql("entity", conn, if_exists="append", index=False)
        # run expectation
        passed, message, details = count_lpa_boundary(
            conn,
            lpa=lpa,
            expected=expected,
            organisation_entity=organisation_entity,
            geometric_relation=geometric_relation,
            comparison_rule=comparison_rule,
        )

    assert (
        passed
    ), f"test expected to pass but it failed expected {details['expected']} but got {details['actual']}"
    assert message, "test should have had a message returned"
    detail_keys = ["actual", "expected"]
    for key in detail_keys:
        assert key in details, f"{key} missing from details"


def test_count_deleted_entities(dataset_path, mocker):
    # define constant parameters
    organisation_entity = 109
    expected = 0

    # load data into sqlite for entity, fact_resource and fact table
    test_entity_data = pd.DataFrame.from_dict(
        {
            "entity": ["1001", "1002"],
            "name": ["test1", "test2"],
            "organisation_entity": [109, 109],
            "reference": ["ref1", "ref2"],
        }
    )

    test_fact_resource_data = pd.DataFrame.from_dict(
        {
            "fact": ["036d2b946bd41", "16bf38800aafd"],
            "resource": ["2f7d900dd48fd02", "2f7d900dd48fd02"],
            "entry_number": ["1", "1"],
        },
    )

    test_fact_data = pd.DataFrame.from_dict(
        {
            "fact": ["036d2b946bd41", "16bf38800aafd"],
            "entity": ["1001", "1001"],
            "field": ["name", "reference"],
            "value": ["abc", "ref1"],
        }
    )

    # mock `pandas.read_csv` to return the mock DataFrame
    mock_df = pd.DataFrame({"resource": ["2f7d900dd48fd02"]})
    mocker.patch("pandas.read_csv", return_value=mock_df)

    with spatialite.connect(dataset_path) as conn:
        # load data into required tables
        test_entity_data.to_sql("entity", conn, if_exists="replace", index=False)
        test_fact_resource_data.to_sql(
            "fact_resource", conn, if_exists="replace", index=False
        )
        test_fact_data.to_sql("fact", conn, if_exists="replace", index=False)

        # run expectation
        passed, message, details = count_deleted_entities(
            conn,
            expected=expected,
            organisation_entity=organisation_entity,
        )

    assert (
        not passed
    ), f"test failed : expected {details['expected']} but got {details['actual']} entities"
    assert message, "test requires a message"

    detail_keys = ["actual", "expected", "entities"]
    for key in detail_keys:
        assert key in details, f"{key} missing from details"
    assert "1002" in details["entities"]


def test_check_columns(dataset_path):
    expected = {
        "entity": [
            "dataset",
            "end_date",
            "entity",
            "entry_date",
            "geojson",
            "geometry",
            "json",
            "name",
            "organisation_entity",
            "point",
            "prefix",
            "reference",
            "start_date",
            "typology",
        ],
        "old_entity": ["old_entity", "entity"],
    }

    with sqlite3.connect(dataset_path) as conn:
        result, message, details = check_columns(conn.cursor(), expected)

        assert result
        assert "2 out of 2 tables had expected columns" in message

        assert details[0]["table"] == "entity"
        assert any(x in details[0]["actual"] for x in expected["entity"])
        assert any(x in details[0]["expected"] for x in expected["entity"])


def test_check_columns_failure(dataset_path):
    expected = {
        "entity": [
            "missing",
            "columns",
            "dataset",
            "end_date",
            "entity",
            "entry_date",
            "geojson",
            "geometry",
            "json",
            "name",
            "organisation_entity",
            "point",
            "prefix",
            "reference",
            "start_date",
            "typology",
        ],
        "old_entity": ["old_entity", "entity"],
    }

    with sqlite3.connect(dataset_path) as conn:
        result, message, details = check_columns(conn.cursor(), expected)
        assert not result
        assert "1 out of 2 tables had expected columns" in message
        assert not details[0]["success"]
        assert "missing" in details[0]["missing"]
        assert "columns" in details[0]["missing"]


def test_duplicate_geometry_check(dataset_path):
    # Add overlapping geometries to db
    with spatialite.connect(dataset_path) as conn:
        # add dummy data
        rows = [
            {
                "entity": 1,
                "geometry": "POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))",
                "organisation_entity": 100,
            },
            {
                "entity": 2,
                "geometry": "POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))",
                "organisation_entity": 101,
            },  # exact geom match to first entity - complete match
            {
                "entity": 3,
                "geometry": "POLYGON((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))",
                "organisation_entity": 102,
            },  # fully encompassed by first entity - one way match
            {
                "entity": 4,
                "geometry": "POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))",
                "organisation_entity": 103,
            },
        ]  # mid section overlap - not enough to trigger overlap
        for row in rows:
            conn.execute(
                "INSERT INTO entity (entity, geometry, organisation_entity) VALUES (?, ?, ?)",
                (row["entity"], row["geometry"], row["organisation_entity"]),
            )
        conn.commit()

    # Now run operation
    result, message, details = duplicate_geometry_check(conn, "geometry")
    conn.close()

    assert not result
    assert message == "There are 1 complete matches and 2 single matches in the dataset"
    assert details["actual"] == 3
    assert details["expected"] == 0

    assert details["complete_matches"][0]["entity_a"] == 1
    assert details["complete_matches"][0]["entity_b"] == 2
    assert details["complete_matches"][0]["organisation_entity_a"] == 100
    assert details["complete_matches"][0]["organisation_entity_b"] == 101

    assert details["single_matches"][1]["entity_a"] == 2
    assert details["single_matches"][1]["entity_b"] == 3
    assert details["single_matches"][1]["organisation_entity_a"] == 101
    assert details["single_matches"][1]["organisation_entity_b"] == 102

    # entity 4 shouldn't have any duplicates
    assert not any(
        row["entity_a"] == 4 or row["entity_b"] == 4
        for row in details["complete_matches"]
    )
    assert not any(
        row["entity_a"] == 4 or row["entity_b"] == 4
        for row in details["single_matches"]
    )


def test_duplicate_geometry_check_point(dataset_path):
    # Add overlapping geometries to db
    with spatialite.connect(dataset_path) as conn:
        # add dummy data
        rows = [
            {
                "entity": 1,
                "point": "POINT(1 1)",
                "organisation_entity": 100,
            },
            {
                "entity": 2,
                "point": "POINT(1 1)",  # duplicate point should flag
                "organisation_entity": 101,
            },
            {
                "entity": 3,
                "point": "POINT(1 2)",
                "organisation_entity": 102,
            },
        ]
        for row in rows:
            conn.execute(
                "INSERT INTO entity (entity, point, organisation_entity) VALUES (?, ?, ?)",
                (row["entity"], row["point"], row["organisation_entity"]),
            )
        conn.commit()

    # Now run operation
    result, message, details = duplicate_geometry_check(conn, "point")
    conn.close()

    assert not result

    assert message == "There are 1 complete matches in the dataset"

    assert details["actual"] == 1
    assert details["expected"] == 0
    assert details["complete_matches"][0]["entity_a"] == 1
    assert details["complete_matches"][0]["entity_b"] == 2
    assert details["complete_matches"][0]["organisation_entity_a"] == 100
    assert details["complete_matches"][0]["organisation_entity_b"] == 101


def test_duplicate_geometry_check_no_dupes(dataset_path):
    # Add overlapping geometries to db
    with spatialite.connect(dataset_path) as conn:
        # add dummy data
        rows = [
            {
                "entity": 1,
                "geometry": "POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))",
                "organisation_entity": 100,
            },
            {
                "entity": 4,
                "geometry": "POLYGON((1 1, 1 3, 3 3, 3 1, 1 1))",
                "organisation_entity": 103,
            },
        ]
        for row in rows:
            conn.execute(
                "INSERT INTO entity (entity, geometry, organisation_entity) VALUES (?, ?, ?)",
                (row["entity"], row["geometry"], row["organisation_entity"]),
            )
        conn.commit()

    # Now run operation
    result, message, details = duplicate_geometry_check(conn, "geometry")
    conn.close()

    assert result
    assert message == "There are no duplicate geometries/points in the dataset"
    assert not details["complete_matches"]
    assert not details["single_matches"]
    assert details["actual"] == 0
    assert details["expected"] == 0
