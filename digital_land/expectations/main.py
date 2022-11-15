from .core import QueryRunner, config_parser, DataQualityException
from datetime import datetime
from .expectations import *  # noqa
import warnings
from csv import DictWriter


def run_expectation_suite(results_file_path, data_path, expectation_suite_yaml):

    now = datetime.now()
    suite_execution_time = now.strftime("%Y%m%d_%H%M%S")
    expectation_suite_config = config_parser(expectation_suite_yaml)

    if expectation_suite_config is None:
        warnings.warn(
            "either empty yaml file being provided or no yaml file being found"
        )
        return

    query_runner = QueryRunner(data_path)

    expectations = expectation_suite_config.get("expectations", None)

    failed_expectation_with_error_severity = 0

    responses = []
    for expectation in expectations:
        arguments = {**expectation}

        response = run_expectation(
            query_runner=query_runner,
            suite_execution_time=suite_execution_time,
            **arguments,
        )

        responses.append(response.to_dict())

        failed_expectation_with_error_severity += response.act_on_failure()

    # data_path_hash = hashlib.sha256(data_path.encode('UTF-8')).hexdigest()
    # file_name = f"{suite_execution_time}_{data_path_hash}.json"
    with open(results_file_path, "w") as f:
        fieldnames = [
            "suite_execution_time",
            "name",
            "description",
            "expectation",
            "severity",
            "result",
            "msg",
            "details",
            "data_name",
            "data_path",
            "expectation_input",
        ]
        dictwriter = DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerows(responses)

    if failed_expectation_with_error_severity > 0:
        raise DataQualityException(
            "One or more expectations with severity RaiseError failed, see results for more details"
        )


def run_expectation(query_runner: QueryRunner, expectation: str, **kwargs):
    return globals()[expectation](query_runner=query_runner, **kwargs)
