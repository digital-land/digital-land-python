import json
import spatialite
import sqlite3
import pytest
import pandas as pd

from digital_land.expectations.operations.dataset import (
    check_columns,
    check_fields_required_after_plan_event,
    count_lpa_boundary,
    count_deleted_entities,
    duplicate_geometry_check,
    duplicate_name_check,
    fetch_active_resources_for_dataset,
    name_is_a_code_check,
    name_is_a_placeholder_check,
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


def test_fetch_active_resources_for_dataset(mocker):
    mock_df = pd.DataFrame(
        {
            "organisation_entity": [101, 101, 102],
            "resource": ["resource_a", "resource_b", "resource_c"],
        }
    )
    mocker.patch("pandas.read_csv", return_value=mock_df)

    result = fetch_active_resources_for_dataset("test-dataset")

    assert result == {101: ["resource_a", "resource_b"], 102: ["resource_c"]}


def test_count_deleted_entities(dataset_path, mocker):
    """Entity with no facts in the active resource is reported as deleted."""
    organisation_entity = 109
    expected = 0

    # load data into sqlite for entity, fact_resource and fact table
    # entity 1001 has facts in the active resource; entity 1002 does not
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

    assert not passed, "expected to fail (1 deleted) but passed"
    assert message
    for key in ["actual", "expected", "entities"]:
        assert key in details, f"{key} missing from details"
    assert details["actual"] == 1
    assert "1002" in details["entities"]


def test_count_deleted_entities_none_deleted(dataset_path, mocker):
    """All entities have facts in the active resource — 0 deleted, check passes."""
    organisation_entity = 109

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
            "fact": ["fact-a", "fact-b"],
            "resource": ["res-active", "res-active"],
            "entry_number": ["1", "2"],
        }
    )

    test_fact_data = pd.DataFrame.from_dict(
        {
            "fact": ["fact-a", "fact-b"],
            "entity": ["1001", "1002"],
            "field": ["name", "name"],
            "value": ["test1", "test2"],
        }
    )

    mock_df = pd.DataFrame({"resource": ["res-active"]})
    mocker.patch("pandas.read_csv", return_value=mock_df)

    with spatialite.connect(dataset_path) as conn:
        test_entity_data.to_sql("entity", conn, if_exists="replace", index=False)
        test_fact_resource_data.to_sql(
            "fact_resource", conn, if_exists="replace", index=False
        )
        test_fact_data.to_sql("fact", conn, if_exists="replace", index=False)

        passed, message, details = count_deleted_entities(
            conn,
            expected=0,
            organisation_entity=organisation_entity,
        )

    assert passed, f"expected 0 deleted but got {details['actual']}"
    assert details["actual"] == 0
    assert details["entities"] == []


def test_count_deleted_entities_org_isolation(dataset_path, mocker):
    """
    Facts for a second organisation's entity in the same resource must not
    cause that entity to be counted as active for the queried organisation.

    Setup: org 109 has entities 1001 (active) and 1002 (deleted). Org 200
    has entity 2001 with a fact in the same resource. Only 1002 should be
    reported as deleted for org 109; entity 2001 must be excluded entirely.
    """
    organisation_entity = 109

    test_entity_data = pd.DataFrame.from_dict(
        {
            "entity": ["1001", "1002", "2001"],
            "name": ["org109 tree A", "org109 tree B", "org200 tree"],
            "organisation_entity": [109, 109, 200],
            "reference": ["T1", "T2", "T3"],
        }
    )

    test_fact_resource_data = pd.DataFrame.from_dict(
        {
            "fact": ["fact-a", "fact-x"],
            "resource": ["res-active", "res-active"],
            "entry_number": ["1", "2"],
        }
    )

    # fact-a belongs to org 109's entity 1001; fact-x belongs to org 200's entity 2001
    test_fact_data = pd.DataFrame.from_dict(
        {
            "fact": ["fact-a", "fact-x"],
            "entity": ["1001", "2001"],
            "field": ["name", "name"],
            "value": ["org109 tree A", "org200 tree"],
        }
    )

    mock_df = pd.DataFrame({"resource": ["res-active"]})
    mocker.patch("pandas.read_csv", return_value=mock_df)

    with spatialite.connect(dataset_path) as conn:
        test_entity_data.to_sql("entity", conn, if_exists="replace", index=False)
        test_fact_resource_data.to_sql(
            "fact_resource", conn, if_exists="replace", index=False
        )
        test_fact_data.to_sql("fact", conn, if_exists="replace", index=False)

        passed, message, details = count_deleted_entities(
            conn,
            expected=1,
            organisation_entity=organisation_entity,
        )

    assert passed, (
        f"expected exactly 1 deleted entity (1002) but got {details['actual']}: "
        f"{details['entities']}"
    )
    assert details["actual"] == 1
    assert "1002" in details["entities"]
    assert "2001" not in details["entities"]


def test_count_deleted_entities_uses_cache_instead_of_http(dataset_path, mocker):
    organisation_entity = 109
    expected = 0

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
        }
    )
    test_fact_data = pd.DataFrame.from_dict(
        {
            "fact": ["036d2b946bd41", "16bf38800aafd"],
            "entity": ["1001", "1001"],
            "field": ["name", "reference"],
            "value": ["abc", "ref1"],
        }
    )

    mock_read_csv = mocker.patch("pandas.read_csv")
    resources_cache = {109: ["2f7d900dd48fd02"]}

    with spatialite.connect(dataset_path) as conn:
        test_entity_data.to_sql("entity", conn, if_exists="replace", index=False)
        test_fact_resource_data.to_sql(
            "fact_resource", conn, if_exists="replace", index=False
        )
        test_fact_data.to_sql("fact", conn, if_exists="replace", index=False)

        passed, _, details = count_deleted_entities(
            conn,
            expected=expected,
            organisation_entity=organisation_entity,
            resources_cache=resources_cache,
        )

    mock_read_csv.assert_not_called()
    assert not passed
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
    assert (
        message
        == "There are 1 complete matches, 2 single matches and 3 any matches in the dataset"
    )
    assert details["actual"] == 6
    assert details["expected"] == 0

    assert details["complete_matches"][0]["entity_a"] == 1
    assert details["complete_matches"][0]["entity_b"] == 2
    assert details["complete_matches"][0]["organisation_entity_a"] == 100
    assert details["complete_matches"][0]["organisation_entity_b"] == 101

    assert details["single_matches"][1]["entity_a"] == 2
    assert details["single_matches"][1]["entity_b"] == 3
    assert details["single_matches"][1]["organisation_entity_a"] == 101
    assert details["single_matches"][1]["organisation_entity_b"] == 102

    # entity 4 has partial overlap with entities 1, 2 and 3 - flagged as any_match only
    assert not any(
        row["entity_a"] == 4 or row["entity_b"] == 4
        for row in details["complete_matches"]
    )
    assert not any(
        row["entity_a"] == 4 or row["entity_b"] == 4
        for row in details["single_matches"]
    )
    assert any(
        row["entity_a"] == 4 or row["entity_b"] == 4 for row in details["any_matches"]
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
                "geometry": "POLYGON((3 3, 3 5, 5 5, 5 3, 3 3))",  # no overlap with entity 1
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
    assert not details["any_matches"]
    assert details["actual"] == 0
    assert details["expected"] == 0


@pytest.fixture
def plan_timetable_path(tmp_path):
    """a minimal built plan-timetable sqlite, matching the dataset entity schema"""
    plan_timetable_path = tmp_path / "plan-timetable.sqlite3"
    with spatialite.connect(plan_timetable_path) as con:
        con.execute(
            """
            CREATE TABLE entity (
                entity INTEGER PRIMARY KEY,
                json JSON,
                reference TEXT
            );
        """
        )
        rows = [
            # consultation started in the past, this plan is gated on
            (1, "started-plan", "proposed-plan-consultation-start", "2024-12-12"),
            # a duplicate row for the same event, the earliest date should win
            (2, "started-plan", "proposed-plan-consultation-start", "2025-01-30"),
            # consultation only planned, no actual-date, so not gated on
            (3, "future-plan", "proposed-plan-consultation-start", ""),
            # a different event which must not open the gate
            (4, "other-event-plan", "scoping-consultation-start", "2020-01-01"),
        ]
        for entity, plan, plan_event, actual_date in rows:
            con.execute(
                "INSERT INTO entity (entity, json, reference) VALUES (?, ?, ?)",
                (
                    entity,
                    json.dumps(
                        {
                            "plan": plan,
                            "plan-event": plan_event,
                            "actual-date": actual_date,
                        }
                    ),
                    f"{plan}-{plan_event}",
                ),
            )
    return plan_timetable_path


def test_check_fields_required_after_plan_event(dataset_path, plan_timetable_path):
    plans = [
        # past consultation start with a blank field, should be flagged
        (1, "started-plan", {"organisations": "local-authority:EXE"}),
        # past consultation start but populated, should not be flagged
        (2, "started-plan-complete", {"organisations": "local-authority:ARU"}),
        # not yet consulted, blank is fine, should not be flagged
        (3, "future-plan", {"organisations": "local-authority:CMD"}),
        # wrong event, blank is fine, should not be flagged
        (4, "other-event-plan", {"organisations": "local-authority:ISL"}),
    ]
    with spatialite.connect(dataset_path) as con:
        for entity, reference, fields in plans:
            if reference == "started-plan-complete":
                fields = {**fields, "period-end-date": "2042-01-01"}
            con.execute(
                "INSERT INTO entity (entity, reference, organisation_entity, json)"
                " VALUES (?, ?, ?, ?)",
                (entity, reference, str(entity), json.dumps(fields)),
            )

    # started-plan-complete has no timetable row, give it one that has started
    with spatialite.connect(plan_timetable_path) as con:
        con.execute(
            "INSERT INTO entity (entity, json, reference) VALUES (?, ?, ?)",
            (
                5,
                json.dumps(
                    {
                        "plan": "started-plan-complete",
                        "plan-event": "proposed-plan-consultation-start",
                        "actual-date": "2024-01-01",
                    }
                ),
                "started-plan-complete-proposed-plan-consultation-start",
            ),
        )

    with spatialite.connect(dataset_path) as con:
        passed, message, details = check_fields_required_after_plan_event(
            conn=con,
            fields=["period-end-date"],
            plan_timetable_path=plan_timetable_path,
            today="2026-08-10",
        )

    assert not passed, message
    assert details["plans_past_event"] == 2
    assert details["failures"] == [
        {
            "organisation": "local-authority:EXE",
            "organisation_entity": "1",
            "reference": "started-plan",
            "field": "period-end-date",
            "actual-date": "2024-12-12",
        }
    ]


def test_check_fields_required_after_plan_event_missing_timetable(
    dataset_path, tmp_path
):
    """a missing artifact must skip the check, never fail the build"""
    with spatialite.connect(dataset_path) as con:
        passed, message, details = check_fields_required_after_plan_event(
            conn=con,
            fields=["period-end-date"],
            plan_timetable_path=tmp_path / "does-not-exist.sqlite3",
        )

    assert passed
    assert details["skipped"] is True
    assert "check skipped" in message


def test_check_fields_required_after_plan_event_null_value(
    dataset_path, plan_timetable_path
):
    """an explicit null must count as missing, not as populated"""
    with spatialite.connect(dataset_path) as con:
        con.execute(
            "INSERT INTO entity (entity, reference, organisation_entity, json)"
            " VALUES (?, ?, ?, ?)",
            (
                1,
                "started-plan",
                "1",
                json.dumps({"organisations": None, "period-end-date": None}),
            ),
        )

    with spatialite.connect(dataset_path) as con:
        passed, message, details = check_fields_required_after_plan_event(
            conn=con,
            fields=["period-end-date"],
            plan_timetable_path=plan_timetable_path,
            today="2026-08-10",
        )

    assert not passed, message
    assert details["failures"] == [
        {
            "organisation": "",
            "organisation_entity": "1",
            "reference": "started-plan",
            "field": "period-end-date",
            "actual-date": "2024-12-12",
        }
    ]


def test_name_is_a_code_check(dataset_path):
    entities = [
        # bare codes, should be flagged
        (1, "A4D-59", "59", "600001", "local-authority:EXE"),
        (2, "CA-3B", "3B", "600002", "local-authority:ARU"),
        (3, "LBO-802", "11/802", "600002", "local-authority:ARU"),
        # descriptive names, should not be flagged
        (4, "CA-WYM", "Wymondham Conservation Area", "600001", "local-authority:EXE"),
        (5, "LBO-GOODGE", "56, GOODGE STREET", "600002", "local-authority:ARU"),
        # no digit, should not be flagged - this is what the lookahead is for
        (6, "CA-NAP", "Napsbury", "600001", "local-authority:EXE"),
        # over 20 characters, should not be flagged
        (7, "CA-LONG", "123456789012345678901", "600001", "local-authority:EXE"),
        # blank, belongs to the missing values issue not this check
        (8, "CA-BLANK", "", "600001", "local-authority:EXE"),
    ]
    with spatialite.connect(dataset_path) as con:
        for entity, reference, name, organisation_entity, _organisation in entities:
            con.execute(
                "INSERT INTO entity (entity, reference, name, organisation_entity)"
                " VALUES (?, ?, ?, ?)",
                (entity, reference, name, organisation_entity),
            )

    with spatialite.connect(dataset_path) as con:
        passed, message, details = name_is_a_code_check(conn=con)

    assert not passed, message
    assert details["actual"] == 3
    assert details["expected"] == 0
    assert details["field"] == "name"
    assert details["failures"] == [
        {
            "organisation_entity": "600001",
            "entity": 1,
            "reference": "A4D-59",
            "name": "59",
        },
        {
            "organisation_entity": "600002",
            "entity": 2,
            "reference": "CA-3B",
            "name": "3B",
        },
        {
            "organisation_entity": "600002",
            "entity": 3,
            "reference": "LBO-802",
            "name": "11/802",
        },
    ]


def test_name_is_a_code_check_all_descriptive(dataset_path):
    with spatialite.connect(dataset_path) as con:
        con.execute(
            "INSERT INTO entity (entity, reference, name, organisation_entity)"
            " VALUES (?, ?, ?, ?)",
            (1, "CA-WYM", "Wymondham Conservation Area", "600001"),
        )

    with spatialite.connect(dataset_path) as con:
        passed, message, details = name_is_a_code_check(conn=con)

    assert passed, message
    assert details["failures"] == []
    assert details["actual"] == 0


@pytest.fixture
def listed_building_path(tmp_path):
    """a minimal built listed-building sqlite, standing in for the Historic England
    dataset fetched during assemble"""
    listed_building_path = tmp_path / "listed-building.sqlite3"
    with spatialite.connect(listed_building_path) as con:
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
                # the listed-building name is itself boilerplate, so there is nothing
                # better to offer and the outline row must not be flagged
                (900003, "No name for this Entry", "3333333"),
                # a blank name is no use either
                (900004, "", "4444444"),
            ],
        )
    return listed_building_path


def _insert_outline(con, entity, reference, name, organisation_entity, listed_building):
    """listed-building is not a core entity column, so it goes in the json blob"""
    con.execute(
        "INSERT INTO entity (entity, reference, name, organisation_entity, json)"
        " VALUES (?, ?, ?, ?, ?)",
        (
            entity,
            reference,
            name,
            organisation_entity,
            json.dumps({"listed-building": listed_building} if listed_building else {}),
        ),
    )


def test_name_is_a_placeholder_check(dataset_path, listed_building_path):
    entities = [
        # placeholder with a real listed-building name, should be flagged
        (1, "LBO-A", "No name for this Entry", "600001", "1234567"),
        # same placeholder in a different case and spacing, should still be flagged
        (2, "LBO-B", "  no   NAME for this ENTRY  ", "600002", "2222222"),
        # a real name appending locating detail, legitimate and not flagged
        (
            3,
            "LBO-C",
            "NORTH LODGE - B1318 (EAST SIDE) GOSFORTH PARK",
            "600001",
            "2222222",
        ),
        # listed-building name is itself a placeholder, nothing better to suggest
        (4, "LBO-D", "No name for this Entry", "600001", "3333333"),
        # listed-building name is blank
        (5, "LBO-E", "No name for this Entry", "600001", "4444444"),
        # listed-building reference matches no record
        (6, "LBO-F", "No name for this Entry", "600001", "9999999"),
        # no listed-building reference at all
        (7, "LBO-G", "No name for this Entry", "600001", None),
        # blank, belongs to the missing values issue not this check
        (8, "LBO-H", "", "600001", "1234567"),
        # contains placeholder-ish words but is a real name, must not substring match
        (9, "LBO-I", "A building with no name plate", "600001", "1234567"),
    ]
    with spatialite.connect(dataset_path) as con:
        for row in entities:
            _insert_outline(con, *row)

    with spatialite.connect(dataset_path) as con:
        passed, message, details = name_is_a_placeholder_check(
            conn=con, listed_building_path=listed_building_path
        )

    assert not passed, message
    assert details["actual"] == 2
    assert details["expected"] == 0
    assert details["field"] == "name"
    assert details["failures"] == [
        {
            "organisation_entity": "600001",
            "entity": 1,
            "reference": "LBO-A",
            "name": "No name for this Entry",
            "listed_building_name": "33, 37 AND 39, BAYFORD GREEN",
        },
        {
            "organisation_entity": "600002",
            "entity": 2,
            "reference": "LBO-B",
            "name": "  no   NAME for this ENTRY  ",
            "listed_building_name": "NORTH LODGE",
        },
    ]


def test_name_is_a_placeholder_check_all_real_names(dataset_path, listed_building_path):
    with spatialite.connect(dataset_path) as con:
        _insert_outline(con, 1, "LBO-C", "NORTH LODGE", "600001", "2222222")

    with spatialite.connect(dataset_path) as con:
        passed, message, details = name_is_a_placeholder_check(
            conn=con, listed_building_path=listed_building_path
        )

    assert passed, message
    assert details["failures"] == []
    assert details["actual"] == 0


def test_name_is_a_placeholder_check_missing_artifact(dataset_path, tmp_path):
    """a missing artifact must skip the check, never fail the build"""
    with spatialite.connect(dataset_path) as con:
        passed, message, details = name_is_a_placeholder_check(
            conn=con,
            listed_building_path=tmp_path / "does-not-exist.sqlite3",
        )

    assert passed
    assert details["skipped"] is True
    assert "check skipped" in message


def test_name_is_a_placeholder_check_placeholders_from_config(
    dataset_path, listed_building_path
):
    """the list is overridable from the parameters cell, so new boilerplate spotted in
    the data can be added without a code release"""
    with spatialite.connect(dataset_path) as con:
        _insert_outline(con, 1, "LBO-J", "No given name", "600001", "1234567")

    with spatialite.connect(dataset_path) as con:
        # not in the default list, so nothing is flagged
        passed, _message, _details = name_is_a_placeholder_check(
            conn=con, listed_building_path=listed_building_path
        )
        assert passed

        passed, _message, details = name_is_a_placeholder_check(
            conn=con,
            listed_building_path=listed_building_path,
            placeholders=["No given name"],
        )

    assert not passed
    assert [failure["name"] for failure in details["failures"]] == ["No given name"]


def test_name_is_a_placeholder_check_single_string_placeholder(
    dataset_path, listed_building_path
):
    """a bare string in config must be treated as one placeholder rather than iterated
    as characters, which would match any single character name"""
    with spatialite.connect(dataset_path) as con:
        _insert_outline(con, 1, "LBO-J", "No given name", "600001", "1234567")
        _insert_outline(con, 2, "LBO-K", "N", "600001", "2222222")

    with spatialite.connect(dataset_path) as con:
        passed, _message, details = name_is_a_placeholder_check(
            conn=con,
            listed_building_path=listed_building_path,
            placeholders="No given name",
        )

    assert not passed
    assert [failure["reference"] for failure in details["failures"]] == ["LBO-J"]


def _insert_named(con, entity, reference, name, organisation_entity):
    con.execute(
        "INSERT INTO entity (entity, reference, name, organisation_entity)"
        " VALUES (?, ?, ?, ?)",
        (entity, reference, name, organisation_entity),
    )


def test_duplicate_name_check(dataset_path):
    entities = [
        # same name twice in one org, should be flagged
        (1, "A4D-1", "Site allocation NSP30", "600001"),
        (2, "A4D-2", "Site allocation NSP30", "600001"),
        # three in one org, should be flagged with a count of 3
        (3, "A4D-3", "District Centre related A4D", "600001"),
        (4, "A4D-4", "District Centre related A4D", "600001"),
        (5, "A4D-5", "District Centre related A4D", "600001"),
        # used once, should not be flagged
        (6, "A4D-6", "Queen's Park", "600001"),
    ]
    with spatialite.connect(dataset_path) as con:
        for entity, reference, name, organisation_entity in entities:
            _insert_named(con, entity, reference, name, organisation_entity)

    with spatialite.connect(dataset_path) as con:
        passed, message, details = duplicate_name_check(conn=con)

    assert not passed, message
    assert details["actual"] == 2
    assert details["expected"] == 0
    assert details["field"] == "name"
    # sorted by organisation then count descending, so the group of 3 comes first
    assert details["failures"] == [
        {
            "organisation_entity": "600001",
            "name": "District Centre related A4D",
            "count": 3,
            "entities": [3, 4, 5],
            "references": ["A4D-3", "A4D-4", "A4D-5"],
        },
        {
            "organisation_entity": "600001",
            "name": "Site allocation NSP30",
            "count": 2,
            "entities": [1, 2],
            "references": ["A4D-1", "A4D-2"],
        },
    ]


def test_duplicate_name_check_is_scoped_per_organisation(dataset_path):
    """the same name in two authorities is coincidence, not a defect neither can fix"""
    with spatialite.connect(dataset_path) as con:
        _insert_named(con, 1, "CA-1", "City Centre", "600001")
        _insert_named(con, 2, "CA-2", "City Centre", "600002")
        _insert_named(con, 3, "CA-3", "City Centre", "600003")

    with spatialite.connect(dataset_path) as con:
        passed, message, details = duplicate_name_check(conn=con)

    assert passed, message
    assert details["failures"] == []


def test_duplicate_name_check_ignores_case_and_spacing(dataset_path):
    """grouping uses _normalise_name, but the raw spelling is what gets reported"""
    with spatialite.connect(dataset_path) as con:
        _insert_named(con, 1, "A4D-1", "POTENTIAL LEISURE PLOTS", "600001")
        _insert_named(con, 2, "A4D-2", "Potential  Leisure Plots", "600001")

    with spatialite.connect(dataset_path) as con:
        passed, message, details = duplicate_name_check(conn=con)

    assert not passed, message
    assert details["actual"] == 1
    failure = details["failures"][0]
    assert failure["count"] == 2
    # the normalised key would be unrecognisable to the authority that wrote it
    assert failure["name"] == "POTENTIAL LEISURE PLOTS"


def test_duplicate_name_check_ignores_blank_names(dataset_path):
    """blanks belong to the missing values issue, and would all match each other"""
    with spatialite.connect(dataset_path) as con:
        _insert_named(con, 1, "CA-1", "", "600001")
        _insert_named(con, 2, "CA-2", "   ", "600001")
        _insert_named(con, 3, "CA-3", None, "600001")

    with spatialite.connect(dataset_path) as con:
        passed, message, details = duplicate_name_check(conn=con)

    assert passed, message
    assert details["failures"] == []


def test_duplicate_name_check_threshold_from_config(dataset_path):
    with spatialite.connect(dataset_path) as con:
        _insert_named(con, 1, "A4D-1", "Pair", "600001")
        _insert_named(con, 2, "A4D-2", "Pair", "600001")
        _insert_named(con, 3, "A4D-3", "Trio", "600001")
        _insert_named(con, 4, "A4D-4", "Trio", "600001")
        _insert_named(con, 5, "A4D-5", "Trio", "600001")

    with spatialite.connect(dataset_path) as con:
        passed, message, details = duplicate_name_check(conn=con, threshold=3)

    assert not passed, message
    assert [failure["name"] for failure in details["failures"]] == ["Trio"]
    assert "3 or more entities" in message
