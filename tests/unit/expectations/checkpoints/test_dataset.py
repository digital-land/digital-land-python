import logging
from unittest.mock import patch

import pytest
from digital_land.expectations.checkpoints.dataset import DatasetCheckpoint
from digital_land.organisation import Organisation


@pytest.fixture
def test_organisations(mocker):
    # mock the init so nothing is loaded
    mocker.patch("digital_land.organisation.Organisation.__init__", return_value=None)
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
        """test loading rules into checckpoint works properly"""
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
        mock_function = mocker.Mock(return_value=True)

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
        required_keys = ["operation", "parameters"]
        for key in required_keys:
            for expectation in checkpoint.expectations:
                assert expectation.get(key) is not None

    def test_load_skips_rule_not_scheduled_today(self, test_organisations, mocker):
        """test loading skips rules not schedule for today"""
        rules = [
            {
                "datasets": "test",
                "organisations": "",
                "operation": "test",
                "parameters": '{"test":"test"}',
                "name": "a saturday only rule",
                "severity": "notice",
                "responsibility": "internal",
                "schedule": "saturday",
            }
        ]
        mocker.patch(
            "digital_land.expectations.checkpoints.dataset.DatasetCheckpoint.operation_factory",
            return_value=mocker.Mock(return_value=True),
        )
        with patch("digital_land.expectations.checkpoints.dataset.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "wednesday"
            checkpoint = DatasetCheckpoint("dataset", "test-path", test_organisations)
            checkpoint.load(rules)

        assert len(checkpoint.expectations) == 0
        assert "a saturday only rule" in checkpoint.skipped_rule_names

    def test_load_includes_rule_scheduled_for_today(self, test_organisations, mocker):
        """test loading includes rules scheduled for today"""
        rules = [
            {
                "datasets": "test",
                "organisations": "",
                "operation": "test",
                "parameters": '{"test":"test"}',
                "name": "a saturday only rule",
                "severity": "notice",
                "responsibility": "internal",
                "schedule": "saturday",
            }
        ]
        mocker.patch(
            "digital_land.expectations.checkpoints.dataset.DatasetCheckpoint.operation_factory",
            return_value=mocker.Mock(return_value=True),
        )
        with patch("digital_land.expectations.checkpoints.dataset.datetime") as mock_dt:
            mock_dt.now.return_value.strftime.return_value = "saturday"
            checkpoint = DatasetCheckpoint("dataset", "test-path", test_organisations)
            checkpoint.load(rules)

        assert len(checkpoint.expectations) == 1

    def test_save_carries_forward_skipped_rule_results(
        self, tmp_path, test_organisations
    ):
        import pyarrow.parquet as pq
        from digital_land.expectations.log import ExpectationLog

        dataset = "test-dataset"
        output_path = tmp_path / "expectations"

        partition_dir = output_path / f"dataset={dataset}"
        partition_dir.mkdir(parents=True)
        previous_log = ExpectationLog(dataset=dataset)
        previous_log.add(
            {
                "organisation": "",
                "name": "Check no duplicate geometries",
                "operation": "duplicate_geometry_check",
                "passed": False,
                "message": "2 duplicates found",
                "details": "",
                "description": "",
                "severity": "notice",
                "responsibility": "internal",
                "parameters": '{"spatial_field": "geometry"}',
            }
        )
        previous_log.save_parquet(output_path)

        checkpoint = DatasetCheckpoint(
            dataset=dataset, file_path="", organisations=test_organisations
        )
        checkpoint.skipped_rule_names = ["Check no duplicate geometries"]
        checkpoint.log.add(
            {
                "organisation": "",
                "name": "A fresh check",
                "passed": True,
                "message": "passed",
                "details": "",
                "description": "",
                "severity": "notice",
                "responsibility": "internal",
                "parameters": "{}",
                "operation": "test",
            }
        )

        checkpoint.save(str(output_path))

        result = pq.ParquetFile(partition_dir / f"{dataset}.parquet").read().to_pylist()
        names = [row["name"] for row in result]
        assert "Check no duplicate geometries" in names
        assert "A fresh check" in names

    def test_save_skipped_rule_no_existing_parquet(self, tmp_path, test_organisations):
        import pyarrow.parquet as pq

        dataset = "test-dataset"
        output_path = tmp_path / "expectations"

        checkpoint = DatasetCheckpoint(
            dataset=dataset, file_path="", organisations=test_organisations
        )
        checkpoint.skipped_rule_names = ["some skipped rule"]
        checkpoint.log.add(
            {
                "organisation": "",
                "name": "A fresh check",
                "passed": True,
                "message": "passed",
                "details": "",
                "description": "",
                "severity": "notice",
                "responsibility": "internal",
                "parameters": "{}",
                "operation": "test",
            }
        )

        checkpoint.save(str(output_path))

        result = (
            pq.ParquetFile(output_path / f"dataset={dataset}" / f"{dataset}.parquet")
            .read()
            .to_pylist()
        )
        assert len(result) == 1
        assert result[0]["name"] == "A fresh check"

    def test_save_to_parquet(self, tmp_path, test_organisations):
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
                "parameters": '{"test":"test"}',
                "operation": "test",
            }
        )

        checkpoint.save(str(output_path))

        assert (
            output_path / f"dataset={dataset}/{dataset}.parquet"
        ).exists(), "csv has not been created in temporaty directory"
