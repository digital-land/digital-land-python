import pandas as pd
import pydantic
from digital_land.expectations.core import (
    transform_df_first_column_into_set,
    config_parser,
    QueryRunner,
    ExpectationResponse,
)


def test_transform_first_col_into_set():
    df = pd.DataFrame(
        {"name": ["Paul", "Tony", "Adam", "Rico"]},
        columns=["name"],
    )
    expected_set = {"Paul", "Tony", "Adam", "Rico"}
    assert expected_set == transform_df_first_column_into_set(df)


def test_query_runner():
    tested_dataset = (
        "tests/expectations/resources_to_test_expectations/lb_single_res.sqlite3"
    )
    query_runner = QueryRunner(tested_dataset)
    sql_query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    result = query_runner.run_query(sql_query)

    expected_result = pd.DataFrame(
        {
            "name": [
                "column_field",
                "dataset_resource",
                "entity",
                "fact",
                "fact_resource",
                "issue",
                "old_entity",
            ]
        },
        columns=["name"],
    )

    pd.testing.assert_frame_equal(result, expected_result)


def test_config_parser():
    file_path = (
        "tests/expectations/resources_to_test_expectations/testing_config_dq_suite.yaml"
    )
    result = config_parser(file_path)

    expected_dictionary = {
        "collection_name": "listed-building",
        "dataset_path_name": "tests/expectations/resources_to_test_expectations/lb_single_res.sqlite3",
        "tables": [
            {
                "tb_name": "fact",
                "tb_expected_min_row_count": 4000,
                "tb_expected_max_row_count": 5000,
                "tb_fields": [
                    {
                        "field_name": "field",
                        "field_duplicity_allowed": True,
                        "field_content_type": "str",
                        "field_allowed_set_of_values": [
                            "description",
                            "entry-date",
                            "geometry",
                            "listed-building-grade",
                            "name",
                        ],
                        "field_row_count_range_per_value": [
                            {
                                "lookup_value": "geometry",
                                "min_row_count": 450,
                                "max_row_count": 550,
                            }
                        ],
                    },
                    {
                        "field_name": "entity",
                        "field_duplicity_allowed": False,
                        "field_content_type": "int",
                        "field_row_count_range_per_value": [
                            {
                                "lookup_value": "42114488",
                                "min_row_count": 8,
                                "max_row_count": 10,
                            },
                            {
                                "lookup_value": "42114489",
                                "min_row_count": 8,
                                "max_row_count": 10,
                            },
                            {
                                "lookup_value": "42114490",
                                "min_row_count": 8,
                                "max_row_count": 10,
                            },
                        ],
                    },
                ],
            }
        ],
        "custom_queries": [
            {"query": "not implemented yet", "expected_result": "not implemented yet"}
        ],
    }

    assert result == expected_dictionary


def test_ExpectationResponse__init__fails_validation():
    try:
        ExpectationResponse(
            expectation_input={"test": "test1"},
            result=True,
            severity="invalid_string",
            msg="Success",
            details=None,
            data_name="test",
            data_path="tes.sqlite3",
            name="test",
            description=None,
            expectation="test",
            entry_date=None,
        )
        assert False
    except pydantic.ValidationError:
        assert True
