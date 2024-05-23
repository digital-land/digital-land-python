#!/usr/bin/env -S py.test -svv
import pytest
from digital_land.pipeline import Pipeline
from digital_land.pipeline import Lookups
from pathlib import Path
from unittest.mock import mock_open, patch


class TestPipeLine:
    def test_load_lookup_removes_eng(self, mocker):
        """
        Very specific test, we are migrating from local-authority-eng
        to local-authority, inf uture this won't be needed butt we have
        to support a transition period
        """

        def mock_file_reader(self, filepath):
            if filepath == "lookup.csv":
                return [
                    {
                        "prefix": "dataset",
                        "organisation": "local-authority-eng:DNC",
                        "reference": "1",
                        "entity": 1,
                    }
                ]
            else:
                return []

        mocker.patch("digital_land.pipeline.Pipeline.file_reader", mock_file_reader)

        p = Pipeline("anything", "stuff")
        for key in p.lookup.keys():
            assert "local-authority-eng" not in key

    def test_skip_patterns(self):
        p = Pipeline("tests/data/pipeline/", "pipeline-one")
        pattern = p.skip_patterns()
        assert isinstance(pattern, list)
        assert "^Unnamed: 0," in pattern

    def test_patches(self):
        p = Pipeline("tests/data/pipeline/", "pipeline-one")
        patches = p.patches()
        assert patches == {"field-one": {"pat": "val"}}

    def test_resource_specific_patches(self):
        p = Pipeline("tests/data/pipeline/", "pipeline-one")
        patches = p.patches("resource-one")
        assert patches == {"field-one": {"something": "else", "pat": "val"}}

    def test_default_fields(self):
        p = Pipeline("tests/data/pipeline", "pipeline-one")
        assert p.default_fields() == {"field-integer": "field-two"}

    def test_resource_specific_default_fields(self):
        p = Pipeline("tests/data/pipeline", "pipeline-one")
        assert p.default_fields("resource-one") == {
            "field-integer": "field-other-integer",
        }

    def test_concatenations(self):
        p = Pipeline("tests/data/pipeline", "pipeline-one")
        concat = p.concatenations()
        assert concat == {
            "combined-field": {"fields": ["field-one", "field-two"], "separator": ". "}
        }

    def test_resource_specific_concatenations(self):
        p = Pipeline("tests/data/pipeline", "pipeline-one")
        concat = p.concatenations("some-resource")
        assert concat == {
            "other-combined-field": {
                "fields": ["field-one", "field-three"],
                "separator": ". ",
            },
            "combined-field": {"fields": ["field-one", "field-two"], "separator": ". "},
        }

    def test_migrate(self):
        p = Pipeline("tests/data/pipeline", "pipeline-one")
        migrations = p.migrations()
        assert migrations == {"field-one": "FieldOne"}

    def test_lookups_get_max_entity_success(self):
        """
        test entity num generation functionality
        :return:
        """

        pipeline_name = "ancient-woodland"
        lookups = Lookups("")
        max_entity_num = lookups.get_max_entity(pipeline_name)

        assert max_entity_num == 0

        entry = {
            "prefix": "ancient-woodland",
            "resource": "",
            "organisation": "government-organisation:D1342",
            "reference": "1",
            "entity": "12344",
        }
        lookups.entries.append(entry)
        expected_entity_num = 12344

        assert lookups.get_max_entity(pipeline_name) == expected_entity_num

        max_entity_num = lookups.get_max_entity(pipeline_name)
        lookups.entity_num_gen.state["current"] = max_entity_num
        lookups.entity_num_gen.state["range_max"] = max_entity_num + 10
        expected_entity_num = 12345

        assert lookups.entity_num_gen.next() == expected_entity_num

    def test_lookups_validate_entry_success(self):
        """
        test validate_entry functionality
        :return:
        """
        lookups = Lookups("")

        entry = {
            "prefix": "ancient-woodland",
            "resource": "",
            "organisation": "government-organisation:D1342",
            "reference": "1",
            "entity": "",
        }

        expected_result = True
        actual_result = lookups.validate_entry(entry)
        assert actual_result == expected_result

    @pytest.mark.parametrize(
        "entry",
        [
            {},
            {"prefix": ""},
            {"prefix": "", "organisation": ""},
            {"prefix": "", "organisation": "", "reference": ""},
            {"prefix": "", "organisation": "", "reference": "", "entity": ""},
            {
                "prefix": "",
                "organisation": "",
                "reference": "",
                "entity": "",
                "resource": "",
            },
        ],
    )
    def test_lookups_validate_entry_failure(self, entry):
        """
        test csv validate_entry functionality for various errors
        :return:
        """
        lookups = Lookups("")

        with pytest.raises(ValueError):
            lookups.validate_entry(entry)

        expected_length = 0
        assert len(lookups.entries) == expected_length

    def test_lookups_add_entry_success(self):
        """
        test add_entry functionality
        :return:
        """
        lookups = Lookups("")

        expected_length = 0
        assert len(lookups.entries) == expected_length

        entry = {
            "prefix": "ancient-woodland",
            "resource": "",
            "organisation": "government-organisation:D1342",
            "reference": "1",
            "entity": "",
        }

        lookups.add_entry(entry)
        expected_length = 1
        assert len(lookups.entries) == expected_length

    @pytest.mark.parametrize(
        "entry",
        [
            {},
            {"prefix": ""},
            {"prefix": "", "organisation": ""},
            {"prefix": "", "organisation": "", "reference": ""},
            {"prefix": "", "organisation": "", "reference": "", "entity": ""},
            {
                "prefix": "",
                "organisation": "",
                "reference": "",
                "entity": "",
                "resource": "",
            },
        ],
    )
    def test_lookups_add_entry_failure(self, entry):
        """
        test add_entry functionality for validation errors
        :return:
        """
        lookups = Lookups("")

        with pytest.raises(ValueError):
            lookups.add_entry(entry)

        expected_length = 0
        assert len(lookups.entries) == expected_length

    def test_lookups_with_old_entity_numbers(self):
        lookups = Lookups("")
        new_lookup = [
            {
                "prefix": "ancient-woodland",
                "resource": "",
                "organisation": "government-organisation:D1342",
                "reference": "2",
                "entity": None,
            }
        ]

        mock_lookups_file = Path("pipeline") / "lookup.csv"
        mock_lookups_file_content = "prefix,resource,organisation,reference,entity\nancient-woodland,,government-organisation:D1342,1,1\n"
        mock_old_entity_file = Path("pipeline") / "old-entity.csv"
        mock_old_entity_file_content = "old-entity,status,entity\n1,301,2\n3,301,4"

        with patch(
            "builtins.open", mock_open(read_data=mock_lookups_file_content), create=True
        ):
            with patch("os.path.exists", return_value=True):
                with patch(
                    "builtins.open",
                    mock_open(read_data=mock_old_entity_file_content),
                    create=True,
                ):
                    lookups.save_csv(
                        mock_lookups_file, new_lookup, mock_old_entity_file
                    )

        assert new_lookup[0]["entity"] == 5

    @pytest.fixture
    def pipeline(self, mocker):
        def mock_file_reader(self, filename):
            if filename == "filter.csv":
                return [
                    {
                        "resource": "resource1",
                        "endpoint": "",
                        "field": "field1",
                        "pattern": "resource_pattern",
                    },
                    {
                        "resource": "",
                        "endpoint": "endpoint1",
                        "field": "field1",
                        "pattern": "endpoint_pattern",
                    },
                    {
                        "resource": "",
                        "endpoint": "",
                        "field": "field2",
                        "pattern": "default_pattern",
                    },
                ]
            return []

        mocker.patch("digital_land.pipeline.Pipeline.file_reader", mock_file_reader)
        return Pipeline("path", "dataset")

    def test_resource_filter_precedence(self, pipeline):
        filters = pipeline.filters(resource="resource1", endpoints="endpoint1")
        assert filters["field1"] == "resource_pattern"

    def test_endpoint_filter_precedence_over_default(self, pipeline):
        filters = pipeline.filters(endpoints=["endpoint1"])
        assert filters["field1"] == "endpoint_pattern"
        assert filters["field2"] == "default_pattern"

    def test_return_only_endpoint_filter(self, pipeline):
        filters = pipeline.filters(endpoints=["endpoint1"])
        assert filters["field1"] == "endpoint_pattern"

    def test_return_only_default_filter(self, pipeline):
        filters = pipeline.filters()
        assert filters["field2"] == "default_pattern"


if __name__ == "__main__":
    pytest.main()
