import pytest
import spatialite
import logging
import pandas as pd
from digital_land.expectations.checkpoints.dataset import DatasetCheckpoint
from digital_land.organisation import Organisation


@pytest.fixture
def test_organisations(mocker):
    # mock the init so nothing is loaded
    mocker.patch("digital_land.organisation.Organisation.__init__", return_value=None)
    # create class
    organisations = Organisation()

    # set up mocked org data, can be changed if the data underneath changes
    org_data = {
        "local-authority:test": {
            "entity": 101,
            "name": "test",
            "prefix": "local-authority",
            "reference": "test",
            "dataset": "local-authority",
            "organisation": "local-authority:test",
            "local-planning-authority": "E0000001",
        },
        "local-authority:test_2": {
            "entity": 102,
            "name": "test_2",
            "prefix": "local-authority",
            "reference": "test_2",
            "dataset": "local-authority",
            "organisation": "local-authority:test_2",
            "local-planning-authority": "E0000002",
        },
    }

    org_lookups = {
        "local-authority:test": "local-authority:test",
        "local-authority:test_2": "local-authority:test_2",
    }

    organisations.organisation = org_data
    organisations.organisation_lookup = org_lookups
    return organisations


def mock_operation_pass(*args, **kwargs):
    passed = True
    message = "this is a mocked operation always returning passed"
    details = {}
    return passed, message, details


class TestDatasetCheckpoint:
    @pytest.mark.parametrize(
        "organisations,expected_count",
        [
            ("local-authority:test", 1),
            ("local-authority:test_2;local-authority:test", 2),
            ("local-authority", 2),
        ],
    )
    def test_get_rule_orgs(self, organisations, expected_count, test_organisations):
        checkpoint = DatasetCheckpoint(
            dataset="dataset",
            file_path="test-path-not-needed",
            organisations=test_organisations,
        )
        rule_orgs = checkpoint.get_rule_orgs({"organisations": organisations})
        assert (
            len(rule_orgs) == expected_count
        ), "incorrect number of organisations provided"

    def test_parse_rule_formates_strings(self, test_organisations):
        """
        a function to get then when rules are parsed with a given organisation dict
        the fields are rendered correctly
        """
        rule = {
            "datasets": "test",
            "organisations": "local-authority:test",
            "operation": "count_lpa_boundary",
            "parameters": '{"organisation-entity":{{ organisation.entity }},"lpa":"{{ organisation.local_planning_authority }}"}',
            "name": "a test expectation for {{ organisation.name }}",
            "desription": "a test decription",
            "severity": "notice",
            "responsibility": "internal",
        }
        checkpoint = DatasetCheckpoint(
            dataset="dataset",
            file_path="test-path-not-needed",
            organisations=test_organisations,
        )
        rule_orgs = checkpoint.get_rule_orgs(rule)
        expectation = checkpoint.parse_rule(rule, rule_orgs[0])
        logging.error(expectation)
        expexted_parameter_values = {"organisation-entity": 101, "lpa": "E0000001"}
        for key in expexted_parameter_values:
            assert (
                expectation["parameters"][key] == expexted_parameter_values[key]
            ), f"Expected {key} to be {expexted_parameter_values[key]} but it was {expectation['parameters'][key]}"

    def test_load_success(self, test_organisations, mocker):
        """test loading rules into checckpoint works properly, shoulld be moved to unit"""
        # define a rule to load in
        rules = [
            {
                "datasets": "test",
                "organisations": "local-authority:test",
                "operation": "test",
                "parameters": '{"test":"test"}',
                "name": "a test expectation",
                "desription": "a test decription",
                "severity": "notice",
                "responsibility": "internal",
            }
        ]
        # need a test on reading from config that it's a string
        # mock operation factory we don't can't about the response
        mock_function = mocker.Mock(
            return_value=True
        )  # Configure the mock to return 10

        # Mock the operation_factory to return the mock_function
        mocker.patch(
            "digital_land.expectations.checkpoints.dataset.DatasetCheckpoint.operation_factory",
            return_value=mock_function,
        )

        checkpoint = DatasetCheckpoint(
            dataset="dataset",
            file_path="test-path-not-needed",
            organisations=test_organisations,
        )
        checkpoint.load(rules)

        assert len(checkpoint.expectations) == 1, checkpoint.expectations
        # check all keys are there
        required_keys = ["operation", "parameters"]
        for key in required_keys:
            for expectation in checkpoint.expectations:
                assert expectation.get(key) is not None

    def test_run_success(
        self, tmp_path, sqlite3_with_entity_tables_path, mocker, test_organisations
    ):
        # load data
        test_entity_data = pd.DataFrame.from_dict({"entity": [1], "name": ["test1"]})
        test_old_entity_data = pd.DataFrame.from_dict(
            {"old_entity": [100], "entity": [10]}
        )
        with spatialite.connect(sqlite3_with_entity_tables_path) as con:
            test_entity_data.to_sql("entity", con, if_exists="append", index=False)
            test_old_entity_data.to_sql(
                "old_entity", con, if_exists="append", index=False
            )

        checkpoint = DatasetCheckpoint(
            dataset="dataset",
            file_path=sqlite3_with_entity_tables_path,
            organisations=test_organisations,
        )
        rules = [
            {
                "datasets": "test",
                "organisations": "local-authority:test",
                "operation": "test",
                "parameters": '{"test":"test"}',
                "name": "a test expectation",
                "desription": "a test decription",
                "severity": "notice",
                "responsibility": "internal",
            }
        ]

        mock_function = mocker.Mock(
            return_value=(True, "this operation is a test and always passes", {})
        )  # Configure the mock to return 10
        # set name for log
        mock_function.__name__ = "test"

        # Mock the operation_factory to return the mock_function
        mocker.patch(
            "digital_land.expectations.checkpoints.dataset.DatasetCheckpoint.operation_factory",
            return_value=mock_function,
        )
        checkpoint.load(rules=rules)
        checkpoint.run()

        log = checkpoint.log

        assert len(log.entries) == 1
        assert log.entries[0]["passed"] is True
        assert log.entries[0]["message"] == "this operation is a test and always passes"

    def test_save_to_csv(self, tmp_path, test_organisations):
        """
        assuming run is successful then the log exists and can be saved
        uses the save method
        """
        output_path = tmp_path / "expectations"
        dataset = "dataset"
        checkpoint = DatasetCheckpoint(
            dataset="dataset",
            file_path="",
            organisations=test_organisations,
        )
        checkpoint.log.add(
            {
                "organisation": "test",
                "name": "A sample",
                "passed": True,
                "message": "hello",
                "details": "",
                "description": "made for test",
                "severity": "notice",
                "respnsibility": "external",
            }
        )

        checkpoint.save(str(output_path))

        assert (
            output_path / f"dataset={dataset}/{dataset}.parquet"
        ).exists(), "csv has not been created in temporaty directory"
