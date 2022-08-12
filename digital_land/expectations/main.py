from .core import QueryRunner, config_parser, DataQualityException
from datetime import datetime
from .expectations import (
    expect_database_to_have_set_of_tables,
    expect_table_to_have_set_of_columns,
    expect_table_row_count_to_be_in_range,
    expect_row_count_for_lookup_value_to_be_in_range,
    expect_field_values_to_be_within_set,
    expect_values_for_field_to_be_unique,
    expect_geoshapes_to_be_valid,
    expect_values_for_a_key_stored_in_json_are_within_a_set,
    expect_keys_in_json_field_to_be_in_set_of_options,
    expect_values_in_field_to_be_within_range,
    expect_custom_query_result_to_be_as_predicted,
)


def run_dq_suite(results_path, sqlite_dataset_path, data_quality_yaml):

    now = datetime.now()
    data_quality_execution_time = now.strftime("%Y%m%d_%H%M%S")

    data_quality_suite_config = config_parser(data_quality_yaml)

    query_runner = QueryRunner(sqlite_dataset_path)

    expectations = data_quality_suite_config.get("expectations", None)

    failed_expectation_with_error_severity = 0

    for expectation in expectations:

        arguments = {**expectation}

        response = run_expectation(
            query_runner=query_runner,
            data_quality_execution_time=data_quality_execution_time,
            **arguments
        )

        response.save_to_file(results_path)
        failed_expectation_with_error_severity += response.act_on_failure()

    if failed_expectation_with_error_severity > 0:
        raise DataQualityException(
            "One or more expectations with severity RaiseError failed, see results for more details"
        )


def run_expectation(query_runner: QueryRunner, expectation_name: str, **kwargs):
    return globals()[expectation_name](query_runner=query_runner, **kwargs)
