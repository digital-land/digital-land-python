#!/usr/bin/env -S py.test -svv

from digital_land.pipeline import Pipeline


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
