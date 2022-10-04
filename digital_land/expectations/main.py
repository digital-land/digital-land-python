from .core import QueryRunner, config_parser, DataQualityException
from datetime import datetime
from .expectations import *  # noqa
import os
import warnings


def run_dq_suite(results_path, sqlite_dataset_path, data_quality_yaml):

    now = datetime.now()
    data_quality_execution_time = now.strftime("%Y%m%d_%H%M%S")
    data_quality_suite_config = config_parser(data_quality_yaml)

    if data_quality_suite_config is None:
        warnings.warn("No expectations provided in yaml file")
        return

    run_directory = os.path.join(
        now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    )

    run_path = os.path.join(results_path, run_directory)
    os.makedirs(run_path)

    query_runner = QueryRunner(sqlite_dataset_path)

    expectations = data_quality_suite_config.get("expectations", None)

    failed_expectation_with_error_severity = 0

    for expectation in expectations:

        arguments = {**expectation}

        response = run_expectation(
            query_runner=query_runner,
            data_quality_execution_time=data_quality_execution_time,
            **arguments,
        )
        # print(response.to_json)

        response.save_to_file(run_path)
        failed_expectation_with_error_severity += response.act_on_failure()

    if failed_expectation_with_error_severity > 0:
        raise DataQualityException(
            "One or more expectations with severity RaiseError failed, see results for more details"
        )


def run_expectation(query_runner: QueryRunner, expectation_name: str, **kwargs):
    return globals()[expectation_name](query_runner=query_runner, **kwargs)
