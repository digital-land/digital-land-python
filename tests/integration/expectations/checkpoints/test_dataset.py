import pytest
import spatialite
import pandas as pd
from digital_land.expectations.checkpoints.dataset import DatasetCheckpoint
from digital_land.expectations.operations.dataset import count_deleted_entities

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


class TestDatasetCheckpoint:
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

    def test_run_with_prefetch_fetches_once_and_injects_cache(
        self, sqlite3_with_entity_tables_path, mocker, test_organisations
    ):
        mock_fetch = mocker.patch(
            "digital_land.expectations.checkpoints.dataset.fetch_active_resources_for_dataset",
            return_value={101: ["resource_a"]},
        )
        mock_function = mocker.create_autospec(
            count_deleted_entities, return_value=(True, "passed", {})
        )
        mock_function.__name__ = "count_deleted_entities"
        mocker.patch(
            "digital_land.expectations.checkpoints.dataset.DatasetCheckpoint.operation_factory",
            return_value=mock_function,
        )

        checkpoint = DatasetCheckpoint(
            dataset="test-dataset",
            file_path=sqlite3_with_entity_tables_path,
            organisations=test_organisations,
        )
        rules = [
            {
                "datasets": "test-dataset",
                "organisations": "local-authority:test;local-authority:test_2",
                "operation": "count_deleted_entities",
                "parameters": '{"organisation_entity": {{ organisation.entity }}, "expected": 0}',
                "name": "test expectation for {{ organisation.name }}",
                "description": "",
                "severity": "notice",
                "responsibility": "internal",
            }
        ]
        checkpoint.load(rules)
        checkpoint.run(prefetch_resources=True)

        # datasette fetched exactly once despite two organisations
        mock_fetch.assert_called_once_with("test-dataset")

        # cache was injected into every operation call
        for call in mock_function.call_args_list:
            assert call.kwargs["resources_cache"] == {101: ["resource_a"]}

        assert len(checkpoint.log.entries) == 2

    def test_run_with_prefetch_does_not_inject_cache_into_incompatible_operations(
        self, sqlite3_with_entity_tables_path, mocker, test_organisations
    ):
        """
        Regression test: resources_cache should only be passed to operations that
        explicitly accept it in their signature. If the inspect.signature check is
        removed, this will fail with:
        TypeError: operation_without_cache() got an unexpected keyword argument 'resources_cache'
        """
        mocker.patch(
            "digital_land.expectations.checkpoints.dataset.fetch_active_resources_for_dataset",
            return_value={101: ["resource_a"]},
        )

        def operation_without_cache(conn, expected: int):
            return True, "passed", {}

        mocker.patch.object(
            DatasetCheckpoint,
            "operation_factory",
            return_value=operation_without_cache,
        )

        checkpoint = DatasetCheckpoint(
            dataset="test-dataset",
            file_path=sqlite3_with_entity_tables_path,
            organisations=test_organisations,
        )
        rules = [
            {
                "datasets": "test-dataset",
                "organisations": "",
                "operation": "operation_without_cache",
                "parameters": '{"expected": 0}',
                "name": "test expectation",
                "description": "",
                "severity": "notice",
                "responsibility": "internal",
            }
        ]
        checkpoint.load(rules)
        checkpoint.run(prefetch_resources=True)
