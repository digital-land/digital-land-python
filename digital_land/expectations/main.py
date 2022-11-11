from .core import QueryRunner, config_parser, DataQualityException
from datetime import datetime
from .expectations import *  # noqa
import os
import warnings
import json
import hashlib
import logging


def run_expectation_suite(results_path, data_path, data_quality_yaml, results_format='json'):

    now = datetime.now()
    suite_execution_time = now.strftime("%Y%m%d_%H%M%S")
    expectation_suite_config = config_parser(data_quality_yaml)

    if expectation_suite_config is None:
        warnings.warn("No expectations provided in yaml file")
        return

    suite_path = os.path.join(
        now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    )

    results_path = os.path.join(results_path, suite_path)
    os.makedirs(results_path, exist_ok=True)

    query_runner = QueryRunner(data_path)

    expectations = expectation_suite_config.get("expectations", None)

    failed_expectation_with_error_severity = 0

    # is format is json cannot add individual lines so collect all expectations as dicts then dump to json
    if results_format == 'json':
        expectations = []

    result_status = 'success'
    
    # for csv can continue to add lines rather than store response so open csv file
    for expectation in expectations:

        arguments = {**expectation}

        response = run_expectation(
            query_runner=query_runner,
            suite_execution_time=suite_execution_time,
            **arguments,
        )

        logging.error(response)

        if not expectation.result:
            result_status == 'fail'

        if results_format == 'json':
            expectations.append(response.to_dict())
            # response.save_to_file(run_path)
        

        failed_expectation_with_error_severity += response.act_on_failure()

    logging.error(expectations)
    if results_format == 'json':
        data_path_hash = hashlib.sha256(data_path.encode('UTF-8')).hexdigest()
        file_name = f"{suite_execution_time}_{result_status}_{data_path_hash}.json"
        with open(file_name, 'w') as f:
            logging.error(file_name)
            json.dump(expectations, f)

    if failed_expectation_with_error_severity > 0:
        raise DataQualityException(
            "One or more expectations with severity RaiseError failed, see results for more details"
        )


def run_expectation(query_runner: QueryRunner, expectation_function: str, **kwargs):
    return globals()[expectation_function](query_runner=query_runner, **kwargs)
