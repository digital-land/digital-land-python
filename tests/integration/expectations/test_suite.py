import pytest
import logging
import os
import yaml
from csv import DictReader

from digital_land.expectations.suite import DatasetExpectationSuite
from digital_land.expectations.core import ExpectationResponse, DataQualityException


def test_config_parser_success(tmp_path):
    yaml_dict = {"expectations": [{"expectation": "test"}]}
    with open(os.path.join(tmp_path, "expectations.yaml"), "w") as yaml_file:
        yaml.dump(yaml_dict, yaml_file)

    results_file_path = os.path.join(tmp_path, "results.csv")
    data_path = os.path.join(tmp_path, "results.csv")
    expectation_suite_yaml = os.path.join(tmp_path, "expectations.yaml")
    suite = DatasetExpectationSuite(
        results_file_path=results_file_path,
        data_path=data_path,
        expectation_suite_yaml=expectation_suite_yaml,
    )

    config = suite.config_parser(expectation_suite_yaml)

    assert config == yaml_dict


def test_config_parser_no_file(tmp_path):

    # set inputs
    results_file_path = os.path.join(tmp_path, "results.csv")
    data_path = os.path.join(tmp_path, "results.csv")
    expectation_suite_yaml = os.path.join(tmp_path, "expectations.yaml")
    suite = DatasetExpectationSuite(
        results_file_path=results_file_path,
        data_path=data_path,
        expectation_suite_yaml=expectation_suite_yaml,
    )

    with pytest.warns(UserWarning):
        config = suite.config_parser(expectation_suite_yaml)

    assert config is None


def test_config_parser_empty_file(tmp_path):

    with open(os.path.join(tmp_path, "expectations.yaml"), "w") as yaml_file:
        pass

    results_file_path = os.path.join(tmp_path, "results.csv")
    data_path = os.path.join(tmp_path, "results.csv")
    expectation_suite_yaml = os.path.join(tmp_path, "expectations.yaml")
    suite = DatasetExpectationSuite(
        results_file_path=results_file_path,
        data_path=data_path,
        expectation_suite_yaml=expectation_suite_yaml,
    )

    with pytest.warns(UserWarning):
        config = suite.config_parser(expectation_suite_yaml)

    assert config is None


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


def test_run_expectation_suite(mocker, tmp_path, test_expectation_function):
    # save yaml
    yaml_dict = {
        "expectations": [
            {
                "severity": "critical",
                "name": "number 1 is bigger than number 2",
                "expectation": "test_expectation_function",
                "number_1": 5,
                "number_2": 2,
            }
        ]
    }
    with open(os.path.join(tmp_path, "expectations.yaml"), "w") as yaml_file:
        yaml.dump(yaml_dict, yaml_file)

    # mock expectation function
    class MockExpectationFunctions:
        def __init__(self, test_expectation_function):
            self.test_expectation_function = test_expectation_function

    mocker.patch(
        "digital_land.expectations.suite.expectations",
        MockExpectationFunctions(test_expectation_function=test_expectation_function),
    )

    # set inputs
    results_file_path = os.path.join(tmp_path, "results.csv")
    data_path = os.path.join(tmp_path, "test.sqlite3")
    expectation_suite_yaml = os.path.join(tmp_path, "expectations.yaml")

    # create and run suite
    suite = DatasetExpectationSuite(
        results_file_path=results_file_path,
        data_path=data_path,
        expectation_suite_yaml=expectation_suite_yaml,
    )
    suite.run_suite()

    response = suite.responses[0]

    assert response.result


@pytest.fixture
def example_response():
    response = ExpectationResponse(
        expectation_input={
            "severity": "critical",
            "name": "number 1 is bigger than number 2",
            "expectation": "test_expectation_function",
            "number_1": 5,
            "number_2": 2,
        },
        result=True,
        severity="critical",
        msg="Success",
        details=None,
        data_name="test",
        data_path="test.sqlite3",
        name="number 1 is bigger than number 2",
        description=None,
        expectation="test_expectation_function",
        entry_date=None,
    )
    return response


def test_save_responses_success(tmp_path, example_response):

    # set uputs
    responses = [example_response]
    results_file_path = os.path.join(tmp_path, "results/results.csv")
    data_path = os.path.join(tmp_path, "test.sqlite3")
    expectation_suite_yaml = os.path.join(tmp_path, "expectations.yaml")

    # create suite and save responses
    suite = DatasetExpectationSuite(
        results_file_path=results_file_path,
        data_path=data_path,
        expectation_suite_yaml=expectation_suite_yaml,
    )
    suite.save_responses(responses=responses)

    # extract results
    actual = []
    with open(os.path.join(tmp_path, "results/results.csv")) as file:
        reader = DictReader(file)
        for row in reader:
            actual.append(row)

    assert responses[0].name == actual[0]["name"]
