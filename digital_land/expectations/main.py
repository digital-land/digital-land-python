from .core import QueryRunner, config_parser, DataQualityException
from datetime import datetime
from .expectations import *  # noqa
import os
import warnings
import json
import hashlib
import logging
from csv import DictWriter


def run_expectation_suite(results_path, data_path, data_quality_yaml):

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
    
    data_path_hash = hashlib.sha256(data_path.encode('UTF-8')).hexdigest()
    file_name = f"{suite_execution_time}_{data_path_hash}.json"
    with open(os.path.join(results_path,file_name),'w') as f:
        fieldnames=[
            'suite_execution_time',
            'name',
            'description',
            'expectation_function',
            'result',
            'msg',
            'details',
            'data_name',
            'data_path',
            'expectation_input'
        ]
        dictwriter = DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerows(responses)

        

    logging.warning(expectations)
    if results_format == 'json':
        data_path_hash = hashlib.sha256(data_path.encode('UTF-8')).hexdigest()
        file_name = f"{suite_execution_time}_{data_path_hash}.json"
        with open(file_name, 'w') as f:
            logging.warning(file_name)
            json.dump(expectations, f)

    if failed_expectation_with_error_severity > 0:
        raise DataQualityException(
            "One or more expectations with severity RaiseError failed, see results for more details"
        )


def run_expectation(query_runner: QueryRunner, expectation_function: str, **kwargs):
    return globals()[expectation_function](query_runner=query_runner, **kwargs)
