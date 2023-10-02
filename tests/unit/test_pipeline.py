#!/usr/bin/env -S py.test -svv
import pytest

from digital_land.pipeline import Pipeline
from digital_land.pipeline import Lookups


def test_columns():
    p = Pipeline("tests/data/pipeline/", "pipeline-one")
    column = p.columns()

    assert column == {
        "dos": "two",
        "due": "one",
        "thirdcolumn": "three",
        "um": "one",
        "un": "one",
        "una": "one",
        "uno": "one",
    }


def test_resource_specific_columns():
    p = Pipeline("tests/data/pipeline/", "pipeline-one")
    column = p.columns("some-resource")

    assert (
        list(column)[0] == "quatro"
    ), "resource specific column 'quatro' should appear first in the returned dict"

    assert column == {
        "dos": "two",
        "due": "one",
        "thirdcolumn": "three",
        "um": "one",
        "un": "one",
        "una": "one",
        "uno": "one",
        "quatro": "four",
    }


def test_skip_patterns():
    p = Pipeline("tests/data/pipeline/", "pipeline-one")
    pattern = p.skip_patterns()
    assert isinstance(pattern, list)
    assert "^Unnamed: 0," in pattern


def test_patches():
    p = Pipeline("tests/data/pipeline/", "pipeline-one")
    patches = p.patches()
    assert patches == {"field-one": {"pat": "val"}}


def test_resource_specific_patches():
    p = Pipeline("tests/data/pipeline/", "pipeline-one")
    patches = p.patches("resource-one")
    assert patches == {"field-one": {"something": "else", "pat": "val"}}


def test_default_fields():
    p = Pipeline("tests/data/pipeline", "pipeline-one")
    assert p.default_fields() == {"field-integer": "field-two"}


def test_resource_specific_default_fields():
    p = Pipeline("tests/data/pipeline", "pipeline-one")
    assert p.default_fields("resource-one") == {
        "field-integer": "field-other-integer",
    }


def test_concatenations():
    p = Pipeline("tests/data/pipeline", "pipeline-one")
    concat = p.concatenations()
    assert concat == {
        "combined-field": {"fields": ["field-one", "field-two"], "separator": ". "}
    }


def test_resource_specific_concatenations():
    p = Pipeline("tests/data/pipeline", "pipeline-one")
    concat = p.concatenations("some-resource")
    assert concat == {
        "other-combined-field": {
            "fields": ["field-one", "field-three"],
            "separator": ". ",
        },
        "combined-field": {"fields": ["field-one", "field-two"], "separator": ". "},
    }


def test_migrate():
    p = Pipeline("tests/data/pipeline", "pipeline-one")
    migrations = p.migrations()
    assert migrations == {"field-one": "FieldOne"}


def test_lookups_get_max_entity_success():
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


def test_lookups_validate_entry_success():
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
def test_lookups_validate_entry_failure(entry):
    """
    test csv validate_entry functionality for various errors
    :return:
    """
    lookups = Lookups("")

    with pytest.raises(ValueError):
        lookups.validate_entry(entry)

    expected_length = 0
    assert len(lookups.entries) == expected_length


def test_lookups_add_entry_success():
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
def test_lookups_add_entry_failure(entry):
    """
    test add_entry functionality for validation errors
    :return:
    """
    lookups = Lookups("")

    with pytest.raises(ValueError):
        lookups.add_entry(entry)

    expected_length = 0
    assert len(lookups.entries) == expected_length
