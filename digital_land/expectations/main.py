from digital_land.expectations.suite import DatasetExpectationSuite


def run_expectation_suite(results_file_path, data_path, expectation_suite_yaml):
    """
    function to run and expectation suite. currently we only run one for
    datasets with yaml inputs. this may need to be updated for other inputs
    """
    expectation_suite = DatasetExpectationSuite(
        results_file_path, data_path, expectation_suite_yaml
    )
    expectation_suite.run_suite()
    expectation_suite.save_responses()
    expectation_suite.act_on_critical_error()
