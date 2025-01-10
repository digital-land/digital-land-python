import spatialite
import pytest
import pandas as pd

from digital_land.expectations.operation import count_lpa_boundary


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
