import pytest
import logging
from digital_land.expectations.suite import DatasetExpectationSuite
from digital_land.expectations.core import DataQualityException


@pytest.fixture
def test_expectation_function():
    def simple_expectation(number_1, number_2, **kwargs):
        result = number_1 > number_2
        if result:
            msg = "Success"
        else:
            msg = "Fail"

        details = {"number_1": number_1, "number_2": number_2}
        return result, msg, details

    return simple_expectation


def test_run_expectation_success(mocker, test_expectation_function):
    # set up mocking
    class MockExpectationFunctions:
        def __init__(self, test_expectation_function):
            self.test_expectation_function = test_expectation_function

    mocker.patch(
        "digital_land.expectations.suite.expectations",
        MockExpectationFunctions(test_expectation_function=test_expectation_function),
    )
    # set up class inputs
    results_file_path = "result.csv"
    data_path = "test/data.sqlite3"
    expectation_suite_yaml = "expectations.yaml"

    # set up function input
    expectation = {
        "severity": "critical",
        "name": "number 1 is bigger than number 2",
        "expectation": "test_expectation_function",
        "number_1": 5,
        "number_2": 2,
    }

    suite = DatasetExpectationSuite(
        results_file_path=results_file_path,
        data_path=data_path,
        expectation_suite_yaml=expectation_suite_yaml,
    )
    response = suite.run_expectation(expectation)
    assert response.result


def test_act_on_critical_error_raises_error():
    # set uputs
    results_file_path = "results/results.csv"
    data_path = "test.sqlite3"
    expectation_suite_yaml = "expectations.yaml"

    # create suite and save responses
    suite = DatasetExpectationSuite(
        results_file_path=results_file_path,
        data_path=data_path,
        expectation_suite_yaml=expectation_suite_yaml,
    )

    try:
        suite.act_on_critical_error(2)
        assert False
    except DataQualityException:
        assert True


def test_act_on_critical_error_does_not_rais_error():
    # set uputs
    results_file_path = "results/results.csv"
    data_path = "test.sqlite3"
    expectation_suite_yaml = "expectations.yaml"

    # create suite and save responses
    suite = DatasetExpectationSuite(
        results_file_path=results_file_path,
        data_path=data_path,
        expectation_suite_yaml=expectation_suite_yaml,
    )

    try:
        suite.act_on_critical_error(0)
        assert True
    except DataQualityException:
        assert False
