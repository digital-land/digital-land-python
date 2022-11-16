import pytest
import os

from digital_land.expectations.main import run_expectation_suite


def test_run_expectation_suite_raises_warning(tmp_path):
    with pytest.warns(UserWarning):
        results_file_path = os.path.join(tmp_path, "results.csv")
        data_path = os.path.join(tmp_path, "data.sqlite3")
        expectation_suite_yaml = os.path.join(tmp_path, "suite.yaml")
        run_expectation_suite(results_file_path, data_path, expectation_suite_yaml)
